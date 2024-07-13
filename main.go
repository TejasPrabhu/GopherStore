package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/tejasprabhu/GopherStore/datamgmt"
)

var (
	server      *Server
	serverMutex sync.Mutex
)

func main() {
	startServer("3000") // Automatically start the server at launch

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
		handleCommand(input)
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
	case "stop":
		stopServer()
	default:
		fmt.Println("Unknown command")
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

    metadata := &datamgmt.Data{
        ID:        "001",
        Filename:  getFileName(filePath),
        Command:   "store",
        OriginID:  "clientID",
        Extension: getFileExtension(filePath),
    }

    fmt.Printf("Sending file %s to %s\n", filePath, destAddr)
    if err := server.sendData(destAddr, metadata, file); err != nil {
        log.Fatalf("Failed to send data: %v\n", err)
    }
}

func getFileName(filePath string) string {
    parts := strings.Split(filePath, "/")
    return parts[len(parts)-1]
}

func getFileExtension(fileName string) string {
    parts := strings.Split(fileName, ".")
    return "." + parts[len(parts)-1]
}
