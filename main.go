package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tejasprabhu/GopherStore/datamgmt"
	"github.com/tejasprabhu/GopherStore/logger"
)

var (
    server      *Server
    serverMutex sync.Mutex
)

func main() {
    port := flag.String("port", "3000", "Port to start the server on")
    flag.Parse()

    startServer(*port)
    go handleCommands()
    select {}
}

func startServer(port string) {
    serverMutex.Lock()
    defer serverMutex.Unlock()

    if server == nil {
        server = NewServer(fmt.Sprintf("0.0.0.0:%s", port))
        go func() {
            if err := server.Start(); err != nil {
                logger.Log.WithError(err).Error("Error starting server")
            }
        }()
        logger.Log.WithField("port", port).Info("Server started")
    } else {
        logger.Log.Warn("Server already running.")
    }
}

func handleCommands() {
    scanner := bufio.NewScanner(os.Stdin)
    logger.Log.Info("Server is running. Enter commands:")
    for scanner.Scan() {
        input := scanner.Text()
        go handleCommand(input)
    }
}

func handleCommand(input string) {
    parts := strings.Fields(input)
    if len(parts) == 0 {
        return
    }

    switch command := parts[0]; command {
    case "send", "fetch", "delete":
        if len(parts) < 3 {
            logger.Log.Warnf("Usage: %s <destination IP:port> <file path>", command)
            return
        }
        handleFileOperation(command, parts[1], parts[2])
    case "stop":
        stopServer()
    default:
        logger.Log.Warn("Unknown command")
    }
}

func handleFileOperation(operation, destAddr, filePath string) {
    if server == nil {
        logger.Log.Error("Server is not running.")
        return
    }

    fileName, fileExt := getFileName(filePath)
    metadata := &datamgmt.Data{
        ID:        "001",
        Filename:  fileName,
        Command:   operation,
        OriginID:  "clientID",
        Extension: fileExt,
    }

    switch operation {
    case "send":
        sendFile(destAddr, metadata, filePath)
    case "fetch", "delete":
        conn, err := server.sendCommand(destAddr, metadata); 
		if err != nil {
            logger.Log.WithError(err).Errorf("Failed to send %s command", operation)
        } 
		if operation == "fetch" {
            processReceivedData(conn)
        }
    }
}

func sendFile(destAddr string, metadata *datamgmt.Data, filePath string) {
    file, err := os.Open(filePath)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to open file")
        return
    }
    defer file.Close()

    if err := server.sendData(destAddr, metadata, file); err != nil {
        logger.Log.WithError(err).Error("Failed to send data")
    }
}

func processReceivedData(conn net.Conn) {
    adapter, err := datamgmt.NewReadStreamAdapter(conn)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to create stream adapter")
        return
    }
    defer adapter.Close() // Ensure this is deferred right after successful creation

    logger.Log.Info("Waiting to read data on client...") // Using Info level for initial waiting message
    for {
        metadata, err := datamgmt.ReadLengthPrefixedData(adapter.GzipReader)
        if err != nil {
            if err == io.EOF {
                logger.Log.Info("EOF reached, closing connection")
                break
            } else {
                logger.Log.WithError(err).Error("Error reading metadata")
                continue
            }
        }

        var data datamgmt.Data
        if err := gob.NewDecoder(bytes.NewReader(metadata)).Decode(&data); err != nil {
            logger.Log.WithError(err).Error("Error decoding metadata")
            continue
        }

        logger.Log.WithFields(map[string]interface{}{
            "command": data.Command, 
            "filename": data.Filename,
        }).Info("Received command")

        // Assuming `handleStoreCommand` is implemented elsewhere and logs its actions
        server.handleStoreCommand(&data, adapter)
    }
}


func stopServer() {
    if server != nil {
        server.Shutdown()
        server = nil
        logger.Log.Info("Server stopped.")
    } else {
        logger.Log.Warn("No server is currently running.")
    }
}

func getFileName(filePath string) (string, string) {
    fileName := filepath.Base(filePath)
    fileExt := filepath.Ext(fileName)
    return fileName[:len(fileName)-len(fileExt)], fileExt[1:]
}