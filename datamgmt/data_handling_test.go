package datamgmt

import (
    "net"
    "testing"
)

func TestCompressionAndEncoding(t *testing.T) {
    local, remote := net.Pipe() // Create a pipe

    go func() {
        defer local.Close() // Close the local connection when done

        // Compression and encoding on the local end
        gzipWriter, encoder := CompressAndEncode(local)
        defer gzipWriter.Close() // Ensure the gzip writer is closed

        testData := Data{
            ID:        "123",
            Filename:  "test.txt",
            Content:   []byte("Hello, world!"),
            OriginID:  "origin",
            Extension: "txt",
        }

        // Encode and compress the data
        if err := encoder.Encode(testData); err != nil {
            t.Errorf("Failed to encode data: %v", err)
            return
        }

        // Flush the gzip writer to ensure all data is written to the pipe
        if err := gzipWriter.Close(); err != nil {
            t.Errorf("Failed to close gzip writer: %v", err)
            return
        }
    }()

    defer remote.Close() // Ensure the remote connection is closed

    // Decompression and decoding on the remote end
    gzipReader, decoder, err := DecompressAndDecode(remote)
    if err != nil {
        t.Errorf("Failed to set up gzip reader: %v", err)
        return
    }
    defer gzipReader.Close() // Ensure the gzip reader is closed

    var decodedData Data
    if err := decoder.Decode(&decodedData); err != nil {
        t.Errorf("Failed to decode data: %v", err)
        return
    }

    // Check if the decoded data matches the original data
    if decodedData.ID != "123" || string(decodedData.Content) != "Hello, world!" {
        t.Errorf("Decoded data mismatch: expected %v, got %v", "123", decodedData.ID)
    }
}
