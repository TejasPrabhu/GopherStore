package p2p

import (
	"log"
	"net"
	"sync"
	"time"
)

type TCPTransport struct {
	listener net.Listener
	address  string
	connWG   sync.WaitGroup
	remoteAddress net.Addr
}

func NewTCPTransport(address string) *TCPTransport {
	return &TCPTransport{address: address}
}

func (t *TCPTransport) Listen() error {
	var err error
	t.listener, err = net.Listen("tcp", t.address)
	if err != nil {
		log.Printf("error: failed to listen on %s, err: %v", t.address, err)
		return err
	}
	log.Printf("info: listening on %s", t.address)
	go t.Accept() // Start accepting connections in a new goroutine
	return nil
}

func (t *TCPTransport) Accept() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				log.Printf("info: listener closed, stopping accept loop: %v", err)
				break // Properly handle listener closure
			}
			log.Printf("error: failed to accept connection, err: %v", err)
			continue
		}
		log.Printf("info: accepted connection from %s", conn.RemoteAddr())
		t.remoteAddress = conn.RemoteAddr()
		t.connWG.Add(1)
		go func(c net.Conn) {
			defer t.connWG.Done()
			t.handleConnection(c)
		}(conn)
	}
	t.connWG.Done() // Decrement the waitgroup counter when the loop exits
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	// Handle the connection
	log.Println("Handling connection")
	defer conn.Close()
	handleClient(conn)
}

func (t *TCPTransport) SendData(data Data, conn net.Conn) error {
    gzipWriter, encoder := compressAndEncode(conn)
    defer gzipWriter.Close()
    if err := encoder.Encode(data); err != nil {
        log.Printf("error: sending data to %s, err: %v", t.remoteAddress, err)
        return err
    }
    return nil
}

func (t *TCPTransport) Close() error {
	err := t.listener.Close()
	if err != nil {
		log.Printf("error: failed to close listener, err: %v", err)
		return err
	}
	t.connWG.Wait()
	log.Println("info: TCP transport closed")
	return nil
}

func (t *TCPTransport) Dial(address string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		log.Printf("error: failed to connect to %s, err: %v", address, err)
		return nil, err
	}
	log.Printf("info: connected to %s", address)
	return conn, nil
}
