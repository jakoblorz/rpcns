package rpcns

import (
	"fmt"
	"net"
)

type Address fmt.Stringer

func NewAddressFromString(addr string) Address {
	return &stringerWrapper{addr}
}

func NewAddressFromAddr(addr net.Addr) Address {
	return addr
}

type stringerWrapper struct {
	s string
}

func (s *stringerWrapper) String() string {
	return s.s
}

type Network interface {
	net.Listener

	Pooler
	Dialer
}

type Dialer interface {
	Dial(Address) (net.Conn, error)
}

type TCP struct {
	Pooler
	li net.Listener
	pl CloseablePooler
}

func (t *TCP) Accept() (net.Conn, error) {
	return t.li.Accept()
}

func (t *TCP) Addr() net.Addr {
	return t.li.Addr()
}

func (*TCP) Dial(addr Address) (net.Conn, error) {
	return (&tcpDialer{}).Dial(addr)
}

func (t *TCP) Close() (err error) {
	err = t.li.Close()
	err = t.pl.Close()
	return
}

type tcpDialer struct{}

func (t *tcpDialer) Dial(addr Address) (net.Conn, error) {
	return net.Dial("tcp", addr.String())
}

func NewTCP(li net.Listener, max int) Network {
	pool := NewCloseablePooler(&tcpDialer{}, max)

	tcp := &TCP{
		Pooler: pool,
		li:     li,
		pl:     pool,
	}
	return tcp
}
