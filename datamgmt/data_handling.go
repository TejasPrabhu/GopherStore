package datamgmt

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"io"
	"net"

    "github.com/tejasprabhu/GopherStore/logger" 
)

// StreamAdapter encapsulates the logger.Logic for managing data streams with compression and encoding.
type StreamAdapter struct {
    Encoder    *gob.Encoder
    Decoder    *gob.Decoder
    GzipWriter *gzip.Writer
    GzipReader *gzip.Reader
    Buffer      *bytes.Buffer  // Buffer to hold stream data for additional processing
    TeeReader   io.Reader      // TeeReader to read and write to buffer simultaneously
}

// NewWriteStreamAdapter creates a new StreamAdapter for writing to a network connection.
func NewWriteStreamAdapter(conn net.Conn) (*StreamAdapter, error) {
    gzipWriter := gzip.NewWriter(conn)
    return &StreamAdapter{
        Encoder:    gob.NewEncoder(gzipWriter),
        GzipWriter: gzipWriter,
    }, nil
}

// NewReadStreamAdapter creates a new StreamAdapter for reading from a network connection.
func NewReadStreamAdapter(conn net.Conn) (*StreamAdapter, error) {
    gzipReader, err := gzip.NewReader(conn)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to create gzip reader")
        return nil, err
    }
    buffer := new(bytes.Buffer)
    teeReader := io.TeeReader(gzipReader, buffer)
    return &StreamAdapter{
        Decoder: gob.NewDecoder(teeReader),
        GzipReader: gzipReader,
        Buffer: buffer,
        TeeReader: teeReader,
    }, nil
}

// Close ensures all resources are properly released.
func (adapter *StreamAdapter) Close() error {
    if adapter.GzipWriter != nil {
        if err := adapter.GzipWriter.Close(); err != nil {
            logger.Log.WithError(err).Error("Failed to close gzip writer")
            return err
        }
    }
    if adapter.GzipReader != nil {
        if err := adapter.GzipReader.Close(); err != nil {
            logger.Log.WithError(err).Error("Failed to close gzip reader")
            return err
        }
    }
    return nil
}

// SendLengthPrefixedData writes data preceded by its length to the writer.
func SendLengthPrefixedData(writer io.Writer, data []byte) error {
    length := uint32(len(data))
    if err := binary.Write(writer, binary.LittleEndian, length); err != nil {
        logger.Log.WithError(err).Error("Failed to write data length")
        return err
    }
    if _, err := writer.Write(data); err != nil {
        logger.Log.WithError(err).Error("Failed to write data")
        return err
    }
    return nil
}

// SendStreamWithSizePrefix sends a stream with its size prefixed to the writer.
func SendStreamWithSizePrefix(writer io.Writer, stream io.Reader) error {
    buffer := new(bytes.Buffer)
    size, err := io.Copy(buffer, stream)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to buffer stream")
        return err
    }
    if err := binary.Write(writer, binary.LittleEndian, uint32(size)); err != nil {
        logger.Log.WithError(err).Error("Failed to write stream size")
        return err
    }
    if _, err := io.Copy(writer, buffer); err != nil {
        logger.Log.WithError(err).Error("Failed to write stream")
        return err
    }
    return nil
}

// ReadLengthPrefixedData reads data from the reader prefixed with its length.
func ReadLengthPrefixedData(reader io.Reader) ([]byte, error) {
    var length uint32
    if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
        logger.Log.WithError(err).Error("Failed to read length prefix")
        return nil, err
    }
    data := make([]byte, length)
    if _, err := io.ReadFull(reader, data); err != nil {
        logger.Log.WithError(err).Error("Failed to read data")
        return nil, err
    }
    return data, nil
}
