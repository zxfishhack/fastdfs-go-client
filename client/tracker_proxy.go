package client

import (
	"context"
	"github.com/lunixbochs/struc"
	"github.com/zxfishhack/fastdfs-go-client/config"
	"github.com/zxfishhack/fastdfs-go-client/proto"
	"golang.org/x/sync/errgroup"
	"log"
	"net"
	"strings"
	"sync"
)

type TrackerProxy struct {
	Client
	lis         net.Listener
	proxyConfig *config.ProxyConfig

	mapMtx   sync.RWMutex
	upToDown map[string]string
	downToUp map[string]string

	ctx    context.Context
	g      *errgroup.Group
	cancel func()
}

func NewTrackerProxy(ctx context.Context, proxyConfig *config.ProxyConfig) (p *TrackerProxy, err error) {
	p = &TrackerProxy{
		proxyConfig: proxyConfig,
		upToDown:    make(map[string]string),
		downToUp:    make(map[string]string),
	}
	for _, s := range proxyConfig.Storage {
		p.upToDown[s.Upstream] = s.Downstream
		p.downToUp[s.Downstream] = s.Upstream
	}
	err = p.Init(ctx, strings.Join(proxyConfig.Tracker.Upstream, ","))
	if err != nil {
		return
	}
	p.lis, err = net.Listen("tcp", proxyConfig.Tracker.Listen)
	if err != nil {
		return
	}
	ctx, p.cancel = context.WithCancel(ctx)
	p.g, p.ctx = errgroup.WithContext(ctx)
	return
}

func (p *TrackerProxy) UpdateStorageMap(storageProxy []*config.StorageProxyConfig) {
	p.mapMtx.Lock()
	defer p.mapMtx.Unlock()
	for _, s := range storageProxy {
		p.upToDown[s.Upstream] = s.Downstream
		p.downToUp[s.Downstream] = s.Upstream
	}
}

func (p *TrackerProxy) Serve() error {
	return p.accept()
}

func (p *TrackerProxy) Close() (err error) {
	p.cancel()
	_ = p.lis.Close()
	return p.g.Wait()
}

func (p *TrackerProxy) accept() (err error) {
	log.Print("proxy begin")
	defer log.Printf("proxy exit")
	for {
		var conn net.Conn
		conn, err = p.lis.Accept()
		if err != nil {
			return
		}
		p.g.Go(func() error {
			e := p.proxy(conn)
			log.Printf("proxy end, peer: %s, error: %s", conn.RemoteAddr(), e.Error())
			return nil
		})
		if err != nil {
			log.Print("build proxy failed, ", err)
			_ = conn.Close()
		}
	}
}

func (p *TrackerProxy) proxy(downstream net.Conn) (err error) {
	var h proto.Header
	defer downstream.Close()
	go func() {
		select {
		case <-p.ctx.Done():
			downstream.Close()
		}
	}()
	for err == nil {
		err = struc.Unpack(downstream, &h)
		if err != nil {
			break
		}
		switch h.Cmd {
		case proto.TrackerProtoCmdStorageGetStatus:
			err = p.storageGetStatus(downstream, h)
		case proto.TrackerProtoCmdStorageGetServerId:
			err = p.storageGetServerId(downstream, h)
		case proto.TrackerProtoCmdServerListOneGroup:
			err = p.rawProxy(downstream, h)
		case proto.TrackerProtoCmdServerListAllGroups:
			err = p.rawProxy(downstream, h)
		case proto.TrackerProtoCmdServerListStorage:
			err = p.serverListStorage(downstream, h)
		case proto.TrackerProtoCmdServerDeleteStorage:
			err = p.serverDeleteStorage(downstream, h)
		case proto.TrackerProtoCmdServerSetTrunkServer:
			err = p.serverSetTrunkServer(downstream, h)
		case proto.TrackerProtoCmdServiceQueryStoreWithoutGroupOne:
			err = p.serviceQueryStoreWithoutGroup(downstream, h)
		case proto.TrackerProtoCmdServiceQueryFetchOne:
			err = p.serviceQueryWithGroupNameFileName(downstream, h)
		case proto.TrackerProtoCmdServiceQueryUpdate:
			err = p.serviceQueryWithGroupNameFileName(downstream, h)
		case proto.TrackerProtoCmdServiceQueryStoreWithGroupOne:
			err = p.serviceQueryStoreWithGroup(downstream, h)
		case proto.TrackerProtoCmdServiceQueryFetchAll:
			err = p.serviceQueryFetchAll(downstream, h)
		case proto.TrackerProtoCmdServiceQueryStoreWithoutGroupAll:
			err = p.serviceQueryStoreWithoutGroup(downstream, h)
		case proto.TrackerProtoCmdServiceQueryStoreWithGroupAll:
			err = p.serviceQueryStoreWithGroup(downstream, h)
		case proto.TrackerProtoCmdServerDeleteGroup:
			err = p.rawProxy(downstream, h)
		case proto.TrackerProtoCmdAddDel:
			err = p.rawProxy(downstream, h)
		case proto.TrackerProtoCmdQueryStatusDel:
			err = p.rawProxy(downstream, h)
		}
	}
	return
}

func (p *TrackerProxy) getUpStreamIp(ip string) string {
	p.mapMtx.RLock()
	defer p.mapMtx.RUnlock()
	if uip, ok := p.downToUp[ip]; ok {
		return uip
	}
	return ip
}

func (p *TrackerProxy) getDownStreamIp(ip string) string {
	p.mapMtx.RLock()
	defer p.mapMtx.RUnlock()
	if dip, ok := p.upToDown[ip]; ok {
		return dip
	}
	return ip
}
