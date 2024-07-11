package p2p

import (
    "testing"
    "time"
)

// TestTCPTransport_ListenAndClose tests the listening and closing functionality of TCPTransport.
func TestTCPTransport_ListenAndClose(t *testing.T) {
    // Create a new TCPTransport instance with a random available port
    transport := NewTCPTransport("127.0.0.1:3000")
    err := transport.Listen()
    if err != nil {
        t.Fatalf("Failed to listen: %v", err)
    }

    // Give some time for the server to start listening
    time.Sleep(time.Second) 

    // Attempt to connect to the listening address
    conn, err := transport.Dial(transport.listener.Addr().String())
    if err != nil {
        t.Fatalf("Failed to dial: %v", err)
    }

    testData := Data{ID: "123", Filename: "test.txt", Content: []byte("Hello, world!")}

    if err := transport.SendData(testData, conn); err != nil {
        t.Fatalf("Failed to send data: %v", err)
    }

    conn.Close()

    // Test closing the transport
    if err := transport.Close(); err != nil {
        t.Errorf("Failed to close transport: %v", err)
    }
}
