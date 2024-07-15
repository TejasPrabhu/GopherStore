package p2p

import (
	"net"
	"sync"
	"time"

	"github.com/tejasprabhu/GopherStore/logger" // Assuming logger is configured for structured logging
)

// TCPTransport handles TCP network operations.
type TCPTransport struct {
	listener      net.Listener
	address       string
	connWG        sync.WaitGroup
	ConnectionsCh chan net.Conn // Channel to pass connections to server handlers
}

// NewTCPTransport creates a new TCP transport system.
func NewTCPTransport(address string) *TCPTransport {
	return &TCPTransport{
		address:       address,
		ConnectionsCh: make(chan net.Conn, 100), // Buffered channel for managing connections
	}
}

// Listen starts the TCP listener on the specified address.
func (t *TCPTransport) Listen() error {
	var err error
	t.listener, err = net.Listen("tcp", t.address)
	if err != nil {
		logger.Log.WithError(err).WithField("address", t.address).Error("Failed to listen on address")
		return err
	}
	logger.Log.WithField("address", t.address).Info("Listening on address")
	go t.Accept()
	return nil
}

// Accept continuously accepts incoming connections and sends them to the processing channel.
func (t *TCPTransport) Accept() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && !netErr.Temporary() {
				logger.Log.WithError(err).Info("Listener closed, stopping accept loop")
				close(t.ConnectionsCh)
				break
			}
			logger.Log.WithError(err).Error("Failed to accept connection")
			continue
		}
		t.ConnectionsCh <- conn // Forward connection to the server for processing
	}
	t.connWG.Wait()
}

// Close shuts down the TCP listener and waits for all pending operations to complete.
func (t *TCPTransport) Close() error {
	if err := t.listener.Close(); err != nil {
		logger.Log.WithError(err).Error("Failed to close listener")
		return err
	}
	t.connWG.Wait()
	logger.Log.Info("TCP transport closed successfully")
	return nil
}

// Dial establishes a new client connection to the specified address with a timeout.
func (t *TCPTransport) Dial(address string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		logger.Log.WithError(err).WithField("address", address).Error("Failed to connect")
		return nil, err
	}
	logger.Log.WithField("address", address).Info("Connected successfully")
	return conn, nil
}
