package datamgmt

import (
	"net"
	"testing"
)


// func TestStreamAdapterCreation(t *testing.T) {
//     local, remote := net.Pipe()

//     defer local.Close()
//     defer remote.Close()

//     // Test writer adapter creation
//     writerAdapter, err := NewWriteStreamAdapter(local)
//     if err != nil {
//         t.Fatalf("Failed to create write stream adapter: %v", err)
//     }
//     defer writerAdapter.Close()

//     // Test reader adapter creation
//     readerAdapter, err := NewReadStreamAdapter(remote)
//     if err != nil {
//         t.Fatalf("Failed to create read stream adapter: %v", err)
//     }
//     defer readerAdapter.Close()

//     // Check if the adapters are not nil
//     if writerAdapter == nil || readerAdapter == nil {
//         t.Fatal("Stream adapters should not be nil")
//     }
// }

func TestSendReceiveLengthPrefixedData(t *testing.T) {
    local, remote := net.Pipe()
    defer local.Close()
    defer remote.Close()

    go func() {
        data := []byte("hello world")
        if err := SendLengthPrefixedData(local, data); err != nil {
            t.Errorf("Failed to send length-prefixed data: %v", err)
        }
    }()

    expectedData, err := ReadLengthPrefixedData(remote)
    if err != nil {
        t.Fatalf("Failed to read length-prefixed data: %v", err)
    }
    if string(expectedData) != "hello world" {
        t.Errorf("Expected 'hello world', got '%s'", string(expectedData))
    }
}


// TestStreamAdapterIntegrity tests the integrity of data transmission using StreamAdapter.
// func TestStreamAdapterIntegrity(t *testing.T) {
//     // Setup a pipe to simulate network connection.
//     local, remote := net.Pipe()

//     // Create a write adapter on the local end.
//     writerAdapter, err := NewWriteStreamAdapter(local)
//     if err != nil {
//         t.Fatalf("Failed to create write stream adapter: %v", err)
//     }
//     defer writerAdapter.Close()

//     // Create a read adapter on the remote end.
//     readerAdapter, err := NewReadStreamAdapter(remote)
//     if err != nil {
//         t.Fatalf("Failed to create read stream adapter: %v", err)
//     }
//     defer readerAdapter.Close()

//     // The data to send.
//     originalData := []byte("Hello, this is a test of the stream adapter integrity!")

//     var wg sync.WaitGroup
//     wg.Add(1)

//     // Use a goroutine to write data.
//     go func() {
//         defer wg.Done()
//         defer local.Close() // Close the write end after sending data.

//         // Send the data with a size prefix.
//         if err := SendStreamWithSizePrefix(writerAdapter.GzipWriter, bytes.NewReader(originalData)); err != nil {
//             t.Errorf("Failed to send data: %v", err)
//         }

//         // Flush the GzipWriter to ensure all data is sent
//         if err := writerAdapter.GzipWriter.Flush(); err != nil {
//             t.Errorf("Failed to flush GzipWriter: %v", err)
//         }
//     }()

//     // Wait for the writing goroutine to finish
//     wg.Wait()

//     // Read the data back on the receiving end.
//     receivedData, err := ReadLengthPrefixedData(readerAdapter.GzipReader)
//     if err != nil {
//         t.Fatalf("Failed to read data: %v", err)
//     }

//     // Close the read end after reading data.
//     remote.Close()

//     // Compare the received data to the original data.
//     if !bytes.Equal(originalData, receivedData) {
//         t.Errorf("Data mismatch: expected %s, got %s", string(originalData), string(receivedData))
//     }
// }