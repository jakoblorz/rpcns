package rpcns

import (
	"io"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
)

type Pooler interface {
	Init(Address)
	Clear(Address)

	Aquire(Address) (*rpc.Client, func(), error)
	Return(Address, *rpc.Client) error
	Call(Address, string, interface{}, interface{}) error

	List() []Address
}

type CloseablePooler interface {
	io.Closer
	Pooler
}

type closeablePooler struct {
	dialer Dialer

	pool map[string]chan *rpc.Client
	lock sync.RWMutex

	max int
}

func (c *closeablePooler) Close() error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for addr := range c.pool {
		c.lock.RUnlock()
		c.Clear(NewAddressFromString(addr))
		c.lock.RLock()
	}

	return nil
}

func (c *closeablePooler) Init(addr Address) {
	c.Clear(addr)

	c.lock.Lock()
	defer c.lock.Unlock()

	c.pool[addr.String()] = make(chan *rpc.Client, c.max)
}

func (c *closeablePooler) Clear(addr Address) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.pool[addr.String()] != nil {
	closer:
		for {
			select {
			case ch, ok := <-c.pool[addr.String()]:
				if !ok {
					return
				}
				ch.Close()
			default:
				break closer
			}
		}
		close(c.pool[addr.String()])
		delete(c.pool, addr.String())
	}
}

func (c *closeablePooler) Aquire(addr Address) (*rpc.Client, func(), error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.pool[addr.String()] != nil {
		select {
		case cl, ok := <-c.pool[addr.String()]:
			if ok {
				return cl, func() {
					c.Return(addr, cl)
				}, nil
			}
		default:
		}
	}

	conn, err := c.dialer.Dial(addr)
	if err != nil {
		return nil, nil, err
	}

	client := jsonrpc.NewClient(conn)
	return client, func() {
		c.Return(addr, client)
	}, nil
}

func (c *closeablePooler) Return(addr Address, cl *rpc.Client) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.pool[addr.String()] == nil {
		c.lock.RUnlock()
		c.Init(addr)
		c.lock.RLock()
	}

	select {
	case c.pool[addr.String()] <- cl:
		return nil
	default:
	}

	return cl.Close()
}

func (c *closeablePooler) Call(addr Address, serviceMethod string, args interface{}, reply interface{}) error {
	cl, returnFn, err := c.Aquire(addr)
	if err != nil {
		return err
	}
	defer returnFn()

	return cl.Call(serviceMethod, args, reply)
}

func (c *closeablePooler) List() (ls []Address) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for addr := range c.pool {
		ls = append(ls, NewAddressFromString(addr))
	}
	return
}

func NewCloseablePooler(d Dialer, max int) CloseablePooler {
	return &closeablePooler{
		dialer: d,
		max:    max,
	}
}
