package main

import (
	// "bufio"
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"net"
	// "strings"
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
	storageService := NewStorageService(address)
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

func (s *Server) fetchData(data *datamgmt.Data, conn net.Conn) {
    reader, err := s.storage.ReadData(data)
    if err != nil {
        log.Printf("Error reading data: %v", err)
        conn.Close() // Ensure to close the connection on error
        return
    } else {
        log.Println("data read successfully")
    }
    defer reader.Close()

    adapter, err := datamgmt.NewWriteStreamAdapter(conn)
    if err != nil {
        log.Printf("Failed to create write stream adapter: %v", err)
        conn.Close() // Ensure to close the connection on error
        return
    } else {
        log.Println("write adapter inside fetcData created")
    }
    defer adapter.Close()

    if err := sendDataToClient(adapter, data, reader); err != nil {
        log.Printf("Failed to send data: %v", err)
        conn.Close() // Ensure to close the connection on error
    } else {
        log.Println("Data sent successfully, closing connection.")
    }

}


func sendDataToClient(adapter *datamgmt.StreamAdapter, data *datamgmt.Data, reader io.Reader) error {
    data.Command = "download"
    metadataBuffer := bytes.Buffer{}
    if err := gob.NewEncoder(&metadataBuffer).Encode(data); err != nil {
        return err
    } else {
        log.Println("encoder created inside fetcData created")
    }


    // Send metadata
    if err := datamgmt.SendLengthPrefixedData(adapter.GzipWriter, metadataBuffer.Bytes()); err != nil {
        return err
    } else {
        log.Println("metadata")
    }

    // Send metadata
    if err := datamgmt.SendStreamWithSizePrefix(adapter.GzipWriter, reader); err != nil {
        return err
    } else {
        log.Println("data sent")
    }

    if err := adapter.GzipWriter.Flush(); err != nil {
        log.Printf("Failed to flush data: %v", err)
    }

    return nil
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

    if err := adapter.GzipWriter.Flush(); err != nil {
        log.Printf("Failed to flush data: %v", err)
    }

    return nil
}

func (s *Server) sendCommand(address string, metadata *datamgmt.Data) (net.Conn, error) {
	conn, err := s.transport.Dial(address)
    if err != nil {
        log.Printf("Failed to connect to %s: %v", address, err)
        return nil, err
    }

	adapter, err := datamgmt.NewWriteStreamAdapter(conn)
    if err != nil {
        log.Printf("Failed to create stream adapter: %v", err)
        return conn, err
    }
    defer adapter.Close()

    // Serialize metadata to get its size
    var metadataBuffer bytes.Buffer
    if err := gob.NewEncoder(&metadataBuffer).Encode(metadata); err != nil {
        return conn, err
    }

    // Send metadata size and metadata
    if err := datamgmt.SendLengthPrefixedData(adapter.GzipWriter, metadataBuffer.Bytes()); err != nil {
        return conn, err
    }

    if err := adapter.GzipWriter.Flush(); err != nil {
        log.Printf("Failed to flush data: %v", err)
    }

    return conn, nil
}


func (s *Server) handleConnection(conn net.Conn) {
	log.Printf("Handling connection from %s", conn.RemoteAddr().String())
    defer conn.Close()

	adapter, err := datamgmt.NewReadStreamAdapter(conn)
	if err != nil {
		log.Printf("Failed to create stream adapter: %v", err)
		return
	} else {
        log.Printf("adapter created")
    }
	defer adapter.Close() // Ensure this is deferred right after successful creation

	for {
		// Repeatedly read commands until the connection is closed
        log.Println("Waiting to read data...")
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

		switch data.Command {
		case "store":
            handleStoreCommand(&data, adapter)
		case "fetch":
            s.fetchData(&data, conn)
		case "delete":
			s.deleteData(&data)
		default:
			log.Printf("Invalid command received: %s", data.Command)
		}
	}
}



func handleStoreCommand(data *datamgmt.Data, adapter *datamgmt.StreamAdapter) {
    log.Println("inside handleStorecmd")
    content, err := datamgmt.ReadLengthPrefixedData(adapter.GzipReader)
    if err != nil {
        log.Printf("Error reading data content: %v", err)
        return
    }
    log.Printf("Data read: %v bytes", len(content))
    byteCOntent := bytes.NewReader(content)
    log.Println(byteCOntent)
    server.storeData(data, byteCOntent)


}

func handleDeleteCommand(data *datamgmt.Data) {
	panic("unimplemented")
}

func handleFetchCommand(data *datamgmt.Data, conn net.Conn) {
	log.Printf("inside fetch")
    server.fetchData(data, conn)
}


