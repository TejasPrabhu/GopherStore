package main

import (
    "bytes"
    "encoding/gob"
    "io"
    "net"
    "sync"

    "github.com/tejasprabhu/GopherStore/datamgmt"
    "github.com/tejasprabhu/GopherStore/logger"
    "github.com/tejasprabhu/GopherStore/p2p"
)

type Server struct {
    transport *p2p.TCPTransport
    storage   *StorageService
    wg        sync.WaitGroup
    quit      chan struct{}
}

func NewServer(address string) *Server {
    storageService := NewStorageService(address)
    transport := p2p.NewTCPTransport(address)
    return &Server{
        transport: transport,
        storage:   storageService,
        quit:      make(chan struct{}),
    }
}

func (s *Server) Start() error {
    logger.Log.Info("Server starting...")
    if err := s.transport.Listen(); err != nil {
        logger.Log.WithError(err).Fatal("Failed to start server")
        return err
    }
    s.wg.Add(1)
    go s.handleConnections()
    return nil
}

func (s *Server) Shutdown() {
    close(s.quit)
    if err :=     s.transport.Close(); err != nil {
        logger.Log.WithError(err).Error("Failed to close connection")
    }
    s.wg.Wait()
    logger.Log.Info("Server shut down.")
}

func (s *Server) handleConnections() {
    for {
        select {
        case <-s.quit:
            return
        case conn, ok := <-s.transport.ConnectionsCh:
            if !ok {
                return
            }
            s.wg.Add(1)
            go s.handleConnection(conn)
        }
    }
}

func (s *Server) handleConnection(conn net.Conn) {
    logger.Log.WithField("address", conn.RemoteAddr().String()).Info("Handling connection")
    defer conn.Close()
    adapter, err := datamgmt.NewReadStreamAdapter(conn)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to create stream adapter")
        return
    }
    defer adapter.Close()

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

        switch data.Command {
        case "send":
            s.handleStoreCommand(&data, adapter)
        case "fetch":
            s.fetchData(&data, conn)
        case "delete":
            s.deleteData(&data)
        default:
            logger.Log.WithField("command", data.Command).Warn("Invalid command received")
        }
    }
}

func (s *Server) handleStoreCommand(data *datamgmt.Data, adapter *datamgmt.StreamAdapter) {
    content, err := datamgmt.ReadLengthPrefixedData(adapter.GzipReader)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to read data content")
        return
    }
    byteContent := bytes.NewReader(content)
    if err := s.storage.StoreData(data, byteContent); err != nil {
        logger.Log.WithError(err).Error("Failed to store data")
    }
}

func (s *Server) fetchData(data *datamgmt.Data, conn net.Conn) {
    reader, err := s.storage.ReadData(data)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to read data")
        return
    }
    defer reader.Close()

    adapter, err := datamgmt.NewWriteStreamAdapter(conn)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to create write stream adapter")
        return
    }
    defer adapter.Close()

    if err := s.sendDataToClient(adapter, data, reader); err != nil {
        logger.Log.WithError(err).Error("Failed to send data")
    }
}

func (s *Server) deleteData(data *datamgmt.Data) {
    if err := s.storage.DeleteData(data); err != nil {
        logger.Log.WithError(err).Error("Failed to delete data")
    }
}

func (s *Server) sendDataToClient(adapter *datamgmt.StreamAdapter, data *datamgmt.Data, reader io.Reader) error {
    logger.Log.Info("Sending data to client")
    data.Command = "download"
    metadataBuffer := bytes.Buffer{}
    if err := gob.NewEncoder(&metadataBuffer).Encode(data); err != nil {
        logger.Log.WithError(err).Error("Failed to encode data")
        return err
    }

    if err := datamgmt.SendLengthPrefixedData(adapter.GzipWriter, metadataBuffer.Bytes()); err != nil {
        logger.Log.WithError(err).Error("Failed to send metadata")
        return err
    }

    if err := datamgmt.SendStreamWithSizePrefix(adapter.GzipWriter, reader); err != nil {
        logger.Log.WithError(err).Error("Failed to send data stream")
        return err
    }

    if err := adapter.GzipWriter.Flush(); err != nil {
        logger.Log.WithError(err).Error("Failed to flush data")
        return err
    }

    logger.Log.Info("Data sent successfully")
    return nil
}

// sendData handles sending data along with metadata to a specified network address.
func (s *Server) sendData(address string, metadata *datamgmt.Data, dataContent io.Reader) error {
    // Establish a network connection to the specified address.
    conn, err := s.transport.Dial(address)
    if err != nil {
        logger.Log.WithError(err).WithField("address", address).Error("Failed to connect")
        return err
    }
    defer conn.Close()  // Ensure the connection is closed after the operation.

    // Create a stream adapter for writing to the network connection.
    adapter, err := datamgmt.NewWriteStreamAdapter(conn)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to create stream adapter")
        return err
    }
    defer adapter.Close()  // Ensure the adapter is closed after the operation.

    // Serialize the metadata into a buffer to determine its size and send it.
    var metadataBuffer bytes.Buffer
    if err := gob.NewEncoder(&metadataBuffer).Encode(metadata); err != nil {
        logger.Log.WithError(err).Error("Failed to encode metadata")
        return err
    }

    // Send the serialized metadata preceded by its length.
    if err := datamgmt.SendLengthPrefixedData(adapter.GzipWriter, metadataBuffer.Bytes()); err != nil {
        logger.Log.WithError(err).Error("Failed to send metadata")
        return err
    }

    // Send the actual data content with a size prefix.
    if err := datamgmt.SendStreamWithSizePrefix(adapter.GzipWriter, dataContent); err != nil {
        logger.Log.WithError(err).Error("Failed to send data content")
        return err
    }

    // Flush the writer to ensure all data is sent.
    if err := adapter.GzipWriter.Flush(); err != nil {
        logger.Log.WithError(err).Error("Failed to flush data")
        return err
    }

    logger.Log.WithField("address", address).Info("Data sent successfully")
    return nil
}

func (s *Server) sendCommand(address string, metadata *datamgmt.Data) (net.Conn, error) {
    // Attempt to establish a connection to the specified address.
    conn, err := s.transport.Dial(address)
    if err != nil {
        logger.Log.WithError(err).WithField("address", address).Error("Failed to connect")
        return nil, err
    }

    // Create a write stream adapter for the connection.
    adapter, err := datamgmt.NewWriteStreamAdapter(conn)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to create stream adapter")
        return nil, err
    }
    defer adapter.Close()

    // Encode the metadata into a buffer to calculate its size.
    var metadataBuffer bytes.Buffer
    if err := gob.NewEncoder(&metadataBuffer).Encode(metadata); err != nil {
        logger.Log.WithError(err).Error("Failed to encode metadata")
        return nil, err
    }

    // Send the metadata with a length prefix.
    if err := datamgmt.SendLengthPrefixedData(adapter.GzipWriter, metadataBuffer.Bytes()); err != nil {
        logger.Log.WithError(err).Error("Failed to send metadata")
        return nil, err
    }

    // Flush any buffered data to ensure all data is sent.
    if err := adapter.GzipWriter.Flush(); err != nil {
        logger.Log.WithError(err).Error("Failed to flush data")
        return nil, err
    }

    logger.Log.WithField("address", address).Info("Command sent successfully")
    return conn, nil
}
