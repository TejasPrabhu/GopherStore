package main

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"net"
	"sync"

	"github.com/tejasprabhu/GopherStore/datamgmt"
	"github.com/tejasprabhu/GopherStore/p2p"
)

// Server encapsulates the TCP transport, storage handling, and peer management.
type Server struct {
	transport *p2p.TCPTransport
	storage   *StorageService
	wg        sync.WaitGroup
	quit      chan struct{}
}

// NewServer creates a new server with the specified TCP address.
func NewServer(address string) *Server {
	storageService := NewStorageService()
	transport := p2p.NewTCPTransport(address)
	return &Server{
		transport: transport,
		storage:   storageService,
		quit:      make(chan struct{}),
	}
}

// Start initiates the server to accept incoming TCP connections and process them.
func (s *Server) Start() error {
	log.Println("Server starting...")
	if err := s.transport.Listen(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
		return err
	}
	s.wg.Add(1)
	go s.handleConnections()
	return nil
}

func (s *Server) Shutdown() {
    close(s.quit)  // Signal all goroutines to stop
    s.transport.Close()  // Close the listener
    s.wg.Wait()  // Wait for all connections to be closed gracefully
    log.Println("Server shut down.")
}


// handleConnections listens for new connections and processes them concurrently.
func (s *Server) handleConnections() {
	for {
		select {
		case <-s.quit:
			return
		case conn, ok := <-s.transport.ConnectionsCh:
			if !ok {
				return // Channel closed
			}
			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}
}




// storeData handles storing data received from the network.
func (s *Server) storeData(data *datamgmt.Data, r io.Reader) {
	if err := s.storage.StoreData(data, r); err != nil {
		log.Printf("Error storing data: %v", err)
	}
}

// fetchData handles fetching data and sending it over the network.
func (s *Server) fetchData(data *datamgmt.Data, conn net.Conn) {
	reader, err := s.storage.ReadData(data)
	if err != nil {
		log.Printf("Error reading data: %v", err)
		return
	}
	defer reader.Close()

	if _, err := io.Copy(conn, reader); err != nil {
		log.Printf("Error sending data back to client: %v", err)
	}
}

// deleteData handles deletion of data.
func (s *Server) deleteData(data *datamgmt.Data) {
	if err := s.storage.DeleteData(data); err != nil {
		log.Printf("Error deleting data: %v", err)
	}
}

// Stop cleanly shuts down the server.
func (s *Server) Stop() {
	close(s.quit)
	s.transport.Close()
	s.wg.Wait()
	log.Println("Server stopped.")
}

func (s *Server) sendData(address string, metadata *datamgmt.Data, dataContent io.Reader) error {
	conn, err := s.transport.Dial(address)
    if err != nil {
        log.Printf("Failed to connect to %s: %v", address, err)
        return err
    }
    defer conn.Close()
	
	adapter, err := datamgmt.NewWriteStreamAdapter(conn)
    if err != nil {
        log.Printf("Failed to create stream adapter: %v", err)
        return err
    }
    defer adapter.Close()

    // Serialize metadata to get its size
    var metadataBuffer bytes.Buffer
    if err := gob.NewEncoder(&metadataBuffer).Encode(metadata); err != nil {
        return err
    }

    // Send metadata size and metadata
    if err := datamgmt.SendLengthPrefixedData(adapter.GzipWriter, metadataBuffer.Bytes()); err != nil {
        return err
    }

    // Send data content size and content
    if err := datamgmt.SendStreamWithSizePrefix(adapter.GzipWriter, dataContent); err != nil {
        return err
    }

    return nil
}

func (s *Server) handleConnection(conn net.Conn) {
    defer s.wg.Done()
    defer conn.Close()

    log.Printf("Handling connection from %s", conn.RemoteAddr().String())

    adapter, err := datamgmt.NewReadStreamAdapter(conn)
    if err != nil {
        log.Printf("Failed to create stream adapter: %v", err)
        return
    }
    defer adapter.Close()

    // Read length-prefixed metadata
    metadata, err := datamgmt.ReadLengthPrefixedData(adapter.GzipReader)
    if err != nil {
        log.Printf("Error reading metadata: %v", err)
        return
    }

    // Deserialize metadata
    var data datamgmt.Data
    if err := gob.NewDecoder(bytes.NewReader(metadata)).Decode(&data); err != nil {
        log.Printf("Error decoding metadata: %v", err)
        return
    }

    // Read and handle actual data content based on command
    switch data.Command {
    case "store":
        content, err := datamgmt.ReadLengthPrefixedData(adapter.GzipReader)
        if err != nil {
            log.Printf("Error reading data content: %v", err)
            return
        }
        s.storeData(&data, bytes.NewReader(content))
	case "fetch":
		// s.fetchData(&metadata, conn)
	case "delete":
		// s.deleteData(&metadata)
	default:
		// log.Printf("Invalid command received: %s", metadata.Command)
    }
}