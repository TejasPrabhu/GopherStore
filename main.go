package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tejasprabhu/GopherStore/datamgmt"
)

var (
	server      *Server
	serverMutex sync.Mutex
)

func main() {
	port := flag.String("port", "3000", "Port to start the server on") // Default port 3000
	flag.Parse()

	startServer(*port) // Automatically start the server at launch

	go handleCommands() // Handle commands in a separate goroutine

	select {} // Block main goroutine to keep the process running
}

func startServer(port string) {
	serverMutex.Lock()
	defer serverMutex.Unlock()

	if server == nil {
		server = NewServer(fmt.Sprintf("0.0.0.0:%s", port))
		go func() {
			err := server.Start()
			if err != nil {
				fmt.Println("Error starting server:", err)
			}
		}()
		fmt.Println("Server started on port", port)
	} else {
		fmt.Println("Server already running.")
	}
}

func handleCommands() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Server is running. Enter commands:")
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

	command := parts[0]
	switch command {
	case "send":
		if len(parts) < 3 {
			fmt.Println("Usage: send <destination IP:port> <file path>")
			return
		}
		sendFile(parts[1], parts[2])
	case "fetch":
		if len(parts) < 3 {
			fmt.Println("Usage: fetch <destination IP:port> <file path>")
			return
		}
		fetchFile(parts[1], parts[2])
    case "delete":
        if len(parts) < 3 {
			fmt.Println("Usage: delete <destination IP:port> <file path>")
			return
		}
		deleteFile(parts[1], parts[2])
	case "stop":
		stopServer()
	default:
		fmt.Println("Unknown command")
	}
}

func sendAck(adapter *datamgmt.StreamAdapter) error {
    ackMessage := struct {
        Acknowledged bool
    }{Acknowledged: true}

    if err := adapter.Encoder.Encode(&ackMessage); err != nil {
        return err
    }
    return nil
}

func deleteFile(destAddr, filePath string) {
    if server == nil {
		fmt.Println("Server is not running.")
		return
	}
	filenameWithExt := filepath.Base(filePath)
	fileExt := filepath.Ext(filenameWithExt)[1:]
	
	fileName := filenameWithExt[:len(filenameWithExt)-len(fileExt)-1]

	fmt.Printf("fileExt : %s", fileExt)

	metadata := &datamgmt.Data{
        ID:        "001",
        Filename:  fileName,
        Command:   "delete",
        OriginID:  "clientID",
        Extension: fileExt,
		// server: 
    }

    fmt.Printf("Deleting file %s from %s\n", filePath, destAddr)
    if _, err := server.sendCommand(destAddr, metadata); err != nil {
        log.Fatalf("Failed to send command: %v\n", err)
    }
}

func fetchFile(destAddr, filePath string) {
	if server == nil {
		fmt.Println("Server is not running.")
		return
	}
	filenameWithExt := filepath.Base(filePath)
	fileExt := filepath.Ext(filenameWithExt)[1:]
	
	fileName := filenameWithExt[:len(filenameWithExt)-len(fileExt)-1]

	fmt.Printf("fileExt : %s", fileExt)

	metadata := &datamgmt.Data{
        ID:        "001",
        Filename:  fileName,
        Command:   "fetch",
        OriginID:  "clientID",
        Extension: fileExt,
		// server: 
    }

	fmt.Printf("Fetching file %s from %s\n", filePath, destAddr)
   conn, err := server.sendCommand(destAddr, metadata)
   if err != nil {
        log.Fatalf("Failed to send data: %v\n", err)
    }

    processReceivedData(conn)

}

func processReceivedData(conn net.Conn)  {
	adapter, err := datamgmt.NewReadStreamAdapter(conn)
	if err != nil {
		log.Printf("Failed to create stream adapter: %v", err)
		return
	} else {
        log.Printf("client reader adapter created")
    }
	defer adapter.Close() // Ensure this is deferred right after successful creation

	for {
		// Repeatedly read commands until the connection is closed
        log.Println("Waiting to read data on client...")
		metadata, err := datamgmt.ReadLengthPrefixedData(adapter.GzipReader)
        println(err)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading metadata: %v", err)
			} else {
                log.Println("EOF reached, closing connection")
            }
			break
		}

		var data datamgmt.Data
		if err := gob.NewDecoder(bytes.NewReader(metadata)).Decode(&data); err != nil {
			log.Printf("Error decoding metadata: %v", err)
			continue
		}

        log.Printf("Received command: %s, Filename: %s", data.Command, data.Filename)

        handleStoreCommand(&data, adapter)

        // Send acknowledgment to server
        // fmt.Fprintln(conn, "Data received and processed")
    
    }
}


func stopServer() {
	if server != nil {
		server.Stop()
		server = nil
		fmt.Println("Server stopped.")
	} else {
		fmt.Println("No server is currently running.")
	}
}


func sendFile(destAddr, filePath string) {
	if server == nil {
		fmt.Println("Server is not running.")
		return
	}
	fmt.Printf("Sending file %s to %s\n", filePath, destAddr)
    file, err := os.Open(filePath)
    if err != nil {
        log.Fatalf("Failed to open file: %v\n", err)
        return
    }
    defer file.Close()

	fileName, extension := getFileName(filePath)

    metadata := &datamgmt.Data{
        ID:        "001",
        Filename:  fileName,
        Command:   "store",
        OriginID:  "clientID",
        Extension: extension,
    }

    fmt.Printf("Sending file %s to %s\n", filePath, destAddr)
    if err := server.sendData(destAddr, metadata, file); err != nil {
        log.Fatalf("Failed to send data: %v\n", err)
    }
}

func getFileName(filePath string) (string, string) {
    parts := strings.Split(filePath, "/")
    fileName := strings.Split(parts[len(parts)-1], ".")
    return fileName[0], fileName[1]
}
