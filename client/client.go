package client

import (
	"context"
	"github.com/zxfishhack/fastdfs-go-client/util"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

var connPool map[string]pool.Pool
var connMtx sync.RWMutex

func init() {
	connPool = make(map[string]pool.Pool)
}

type Client struct {
	pools []pool.Pool
	addrs []string
}

type connBuilder struct {
	net.Dialer
	ctx  context.Context
	addr string
}

func (cb *connBuilder) getConn() (nc net.Conn, err error) {
	var ctx context.Context
	if cb.ctx == nil {
		ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	} else {
		ctx, _ = context.WithTimeout(cb.ctx, 5*time.Second)
	}
	return cb.DialContext(ctx, "tcp", cb.addr)
}

func (c *Client) Init(ctx context.Context, addrs string) (err error) {
	addrList := strings.Split(addrs, ",")
	var addrWithLockList []string
	func() {
		connMtx.RLock()
		defer connMtx.RUnlock()
		for _, addr := range addrList {
			if p, ok := connPool[addr]; ok {
				c.pools = append(c.pools, p)
			} else {
				addrWithLockList = append(addrWithLockList, addr)
			}
		}
	}()

	connMtx.Lock()
	defer connMtx.Unlock()
	for _, addr := range addrWithLockList {
		if p, ok := connPool[addr]; ok {
			c.pools = append(c.pools, p)
		} else {
			cb := &connBuilder{
				Dialer: net.Dialer{
					Timeout: clientConfig.ConnTimeout,
				},
				ctx:  ctx,
				addr: addr,
			}
			p, err = pool.NewChannelPool(1, 20, cb.getConn)
			if err != nil {
				return
			}
			connPool[addr] = p
			c.pools = append(c.pools, p)
		}
	}
	return
}

func (c *Client) call(data interface{}, r util.Reader) (recvBuf []byte, status int, err error) {
	var nc net.Conn

	for _, p := range c.pools {
		nc, err = p.Get()
		if err != nil {
			return
		}
		recvBuf, status, err = util.Call(nc, data, r)
		_ = nc.Close()
		if err == nil {
			return
		}
	}
	return
}

func (c *Client) callWithInterface(data interface{}, r util.Reader, res interface{}) (status int, err error) {
	var nc net.Conn

	for _, p := range c.pools {
		nc, err = p.Get()
		if err != nil {
			return
		}
		status, err = util.CallWithInterface(nc, data, r, res)
		_ = nc.Close()
		if err == nil {
			return
		}
	}
	return
}

func (c *Client) callWithWriter(data interface{}, r util.Reader, w io.Writer) (status int, err error) {
	var nc net.Conn

	for _, p := range c.pools {
		nc, err = p.Get()
		if err != nil {
			return
		}
		status, err = util.CallWithWriter(nc, data, r, w)
		_ = nc.Close()
		if err == nil {
			return
		}
	}
	return
}
