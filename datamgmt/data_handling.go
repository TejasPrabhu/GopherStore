package datamgmt

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"io"
	"net"
)

// StreamAdapter encapsulates the logic for compressing, encoding, and buffering data streams.
type StreamAdapter struct {
    Encoder     *gob.Encoder
    Decoder     *gob.Decoder
    GzipWriter  *gzip.Writer
    GzipReader  *gzip.Reader
    Buffer      *bytes.Buffer  // Buffer to hold stream data for additional processing
    TeeReader   io.Reader      // TeeReader to read and write to buffer simultaneously
}

// NewWriteStreamAdapter initializes a stream adapter for writing.
func NewWriteStreamAdapter(conn net.Conn) (*StreamAdapter, error) {
    gzipWriter := gzip.NewWriter(conn)
    return &StreamAdapter{
        Encoder: gob.NewEncoder(gzipWriter),
        GzipWriter: gzipWriter,
    }, nil
}

// NewReadStreamAdapter initializes a stream adapter for reading, with TeeReader.
func NewReadStreamAdapter(conn net.Conn) (*StreamAdapter, error) {
    gzipReader, err := gzip.NewReader(conn)
    if err != nil {
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

// Close closes all resources used by the StreamAdapter.
func (adapter *StreamAdapter) Close() error {
    if adapter.GzipWriter != nil {
        if err := adapter.GzipWriter.Close(); err != nil {
            return err
        }
    }
    if adapter.GzipReader != nil {
        if err := adapter.GzipReader.Close(); err != nil {
            return err
        }
    }
    return nil
}

func ReadLengthPrefixedData(reader io.Reader) ([]byte, error) {
    var length uint32
    if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
        return nil, err
    }
    data := make([]byte, length)
    _, err := io.ReadFull(reader, data)
    return data, err
}


// Helper to send length-prefixed data
func SendLengthPrefixedData(writer io.Writer, data []byte) error {
    length := uint32(len(data))
    if err := binary.Write(writer, binary.LittleEndian, length); err != nil {
        return err
    }
    _, err := writer.Write(data)
    return err
}

// Helper to send stream with size prefix
func SendStreamWithSizePrefix(writer io.Writer, stream io.Reader) error {
    buf := new(bytes.Buffer)
    size, err := io.Copy(buf, stream) // Buffer the stream to calculate size
    if err != nil {
        return err
    }

    if err := binary.Write(writer, binary.LittleEndian, uint32(size)); err != nil {
        return err
    }
    _, err = io.Copy(writer, buf) // Write the buffered content
    return err
}