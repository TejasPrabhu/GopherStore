package p2p

import (
	"io"
	"net"
)

type Transport interface {
	Listen(address string) error
	Dial(address string) (io.ReadWriteCloser, error)
	Accepct() (io.ReadWriteCloser, error)
	Close() error
	SendData(data Data, conn net.Conn) error
}
