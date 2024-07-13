package p2p

import (
	"log"
	"net"
	"sync"
	"time"
)

type TCPTransport struct {
	listener      net.Listener
	address       string
	connWG        sync.WaitGroup
	ConnectionsCh chan net.Conn // Channel to pass connections to server
}

func NewTCPTransport(address string) *TCPTransport {
	return &TCPTransport{
		address:       address,
		ConnectionsCh: make(chan net.Conn, 100), // Buffered channel for handling connections
	}
}

func (t *TCPTransport) Listen() error {
	var err error
	t.listener, err = net.Listen("tcp", t.address)
	if err != nil {
		log.Printf("error: failed to listen on %s, err: %v", t.address, err)
		return err
	}
	log.Printf("info: listening on %s", t.address)
	go t.Accept()
	return nil
}

func (t *TCPTransport) Accept() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && !netErr.Temporary() {
				log.Printf("info: listener closed, stopping accept loop: %v", err)
				close(t.ConnectionsCh)
				break
			}
			log.Printf("error: failed to accept connection, err: %v", err)
			continue
		}
		t.ConnectionsCh <- conn // Send accepted connection to server
	}
	t.connWG.Wait()
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
