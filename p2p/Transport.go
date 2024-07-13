package p2p

import (
	"io"
)

type Transport interface {
	Listen(address string) error
	Dial(address string) (io.ReadWriteCloser, error)
	Accepct() (io.ReadWriteCloser, error)
	Close() error
	// SendData(data dataData, conn net.Conn) error
}
