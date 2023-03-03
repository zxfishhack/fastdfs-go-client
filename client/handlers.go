package client

import (
	"bytes"
	"github.com/lunixbochs/struc"
	. "github.com/zxfishhack/fastdfs-go-client/proto"
	"github.com/zxfishhack/fastdfs-go-client/util"
	"gopkg.in/fatih/pool.v2"
	"net"
	"strings"
)

func (p *TrackerProxy) getString(c net.Conn, length int) (str string, err error) {
	var buf []byte
	buf, err = util.TimeoutReadN(c, DefaultRWTimeout, length)
	if err != nil {
		return
	}
	str = strings.TrimRight(string(buf), "\x00")
	return
}

func (p *TrackerProxy) rawProxy(downstream net.Conn, h Header) (err error) {
	var buf []byte
	if h.PkgLen > 0 {
		buf, err = util.TimeoutReadN(downstream, DefaultRWTimeout, h.PkgLen)
		if err != nil {
			return
		}
	}
	for _, po := range p.pools {
		var nc net.Conn
		nc, err = po.Get()
		if err != nil {
			continue
		}
		err = struc.Pack(nc, &h)
		if err != nil {
			if pc, ok := nc.(*pool.PoolConn); ok {
				pc.MarkUnusable()
			}
			_ = nc.Close()
			continue
		}
		_, err = nc.Write(buf)
		if err != nil {
			if pc, ok := nc.(*pool.PoolConn); ok {
				pc.MarkUnusable()
			}
			_ = nc.Close()
			continue
		}
		err = struc.Unpack(nc, &h)
		if err != nil {
			if pc, ok := nc.(*pool.PoolConn); ok {
				pc.MarkUnusable()
			}
			_ = nc.Close()
			continue
		}
		var outbuf []byte
		outbuf, err = util.TimeoutReadN(nc, DefaultRWTimeout, h.PkgLen)
		if err != nil {
			if pc, ok := nc.(*pool.PoolConn); ok {
				pc.MarkUnusable()
			}
			_ = nc.Close()
			continue
		}
		//以下是回复段，若出错，只能break
		err = struc.Pack(downstream, &h)
		if err != nil {
			break
		}
		_, err = downstream.Write(outbuf)
		break
	}
	return
}

func (p *TrackerProxy) storageGetStatus(downstream net.Conn, h Header) (err error) {
	req := GetStorageStatusReq{Header: h}
	req.GroupName, err = p.getString(downstream, GroupNameMaxLen)
	if err != nil {
		return
	}
	req.StorageId, err = p.getString(downstream, h.PkgLen-GroupNameMaxLen)
	if err != nil {
		return
	}
	//将客户端传入的IP地址替换为上游服务器使用的
	req.StorageId = p.getUpStreamIp(req.StorageId)
	req.PkgLen = GroupNameMaxLen + len(req.StorageId)
	var resp StorageBrief
	var status int
	var recvBuf []byte
	recvBuf, status, err = p.call(&req, nil)
	if err != nil {
		return
	}
	h = Header{
		Status: status,
		PkgLen: len(recvBuf),
	}
	err = struc.Pack(downstream, &h)
	if status != 0 || err != nil {
		return
	}
	if len(recvBuf) > 0 {
		err = struc.Unpack(bytes.NewReader(recvBuf), &resp)
		if err != nil {
			return
		}
		//将上游服务器使用的IP替换为客户端使用的
		resp.IpAddr = p.getDownStreamIp(resp.IpAddr)
		err = struc.Pack(downstream, &resp)
	}
	return
}

func (p *TrackerProxy) storageGetServerId(downstream net.Conn, h Header) (err error) {
	req := GetServerIdReq{Header: h}
	req.GroupName, err = p.getString(downstream, GroupNameMaxLen)
	if err != nil {
		return
	}
	req.StorageId, err = p.getString(downstream, h.PkgLen-GroupNameMaxLen)
	if err != nil {
		return
	}
	//将客户端传入的IP地址替换为上游服务器使用的
	req.StorageId = p.getUpStreamIp(req.StorageId)
	req.PkgLen = GroupNameMaxLen + len(req.StorageId)
	var status int
	var recvBuf []byte
	recvBuf, status, err = p.call(&req, nil)
	if err != nil {
		return
	}
	h = Header{
		Status: status,
		PkgLen: len(recvBuf),
	}
	err = struc.Pack(downstream, &h)
	if status != 0 || err != nil {
		return
	}
	_, err = downstream.Write(recvBuf)
	return
}

func (p *TrackerProxy) serverListStorage(downstream net.Conn, h Header) (err error) {
	req := ListServersReq{Header: h}
	req.GroupName, err = p.getString(downstream, GroupNameMaxLen)
	if err != nil {
		return
	}
	if h.PkgLen > GroupNameMaxLen {
		req.StorageId, err = p.getString(downstream, h.PkgLen-GroupNameMaxLen)
		if err != nil {
			return
		}
	}
	//将客户端传入的IP地址替换为上游服务器使用的
	req.StorageId = p.getUpStreamIp(req.StorageId)
	req.PkgLen = GroupNameMaxLen + len(req.StorageId)
	var status int
	var recvBuf []byte
	recvBuf, status, err = p.call(&req, nil)
	if err != nil {
		return
	}
	h = Header{
		Status: status,
		PkgLen: len(recvBuf),
	}
	err = struc.Pack(downstream, &h)
	if status != 0 || err != nil {
		return
	}
	b := bytes.NewReader(recvBuf)
	for b.Len() > 0 && err == nil {
		storage := &StorageStat{}
		err = struc.Unpack(b, storage)
		if err != nil {
			break
		}
		storage.IpAddr = p.getDownStreamIp(strings.TrimRight(storage.IpAddr, "\x00"))
		err = struc.Pack(downstream, &storage)
	}
	return
}

func (p *TrackerProxy) serverDeleteStorage(downstream net.Conn, h Header) (err error) {
	req := DeleteStorageReq{Header: h}
	req.GroupName, err = p.getString(downstream, GroupNameMaxLen)
	if err != nil {
		return
	}
	req.StorageId, err = p.getString(downstream, h.PkgLen-GroupNameMaxLen)
	if err != nil {
		return
	}
	//将客户端传入的IP地址替换为上游服务器使用的
	req.StorageId = p.getUpStreamIp(req.StorageId)
	req.PkgLen = GroupNameMaxLen + len(req.StorageId)
	var status int
	_, status, err = p.call(&req, nil)
	if err != nil {
		return
	}
	h = Header{
		Status: status,
	}
	err = struc.Pack(downstream, &h)
	return
}

func (p *TrackerProxy) serverSetTrunkServer(downstream net.Conn, h Header) (err error) {
	req := &SetTrunkServerReq{Header: h}
	req.GroupName, err = p.getString(downstream, GroupNameMaxLen)
	if err != nil {
		return
	}
	req.StorageId, err = p.getString(downstream, h.PkgLen-GroupNameMaxLen)
	if err != nil {
		return
	}
	//将客户端传入的IP地址替换为上游服务器使用的
	req.StorageId = p.getUpStreamIp(req.StorageId)
	req.PkgLen = GroupNameMaxLen + len(req.StorageId)
	var nc net.Conn
	var status int
	var recvBuf []byte
	for _, p := range p.pools {
		nc, err = p.Get()
		if err != nil {
			continue
		}
		recvBuf, status, err = util.Call(nc, req, nil)
		if err != nil {
			if pc, ok := nc.(*pool.PoolConn); ok {
				pc.MarkUnusable()
			}
			_ = nc.Close()
			continue
		}
		if status == EOPNOTSUPP {
			_ = nc.Close()
			continue
		}
		if status == EALREADY {
			_ = nc.Close()
			break
		}
		if status != 0 {
			_ = nc.Close()
			break
		}
		_ = nc.Close()
		break
	}
	if err == nil {
		h.Status = status
		h.PkgLen = len(recvBuf)
		err = struc.Pack(downstream, &h)
		if err == nil && len(recvBuf) > 0 {
			_, err = downstream.Write(recvBuf)
		}
	}
	return
}

func (p *TrackerProxy) serviceQueryStoreWithGroup(downstream net.Conn, h Header) (err error) {
	req := FileObject{Header: h}
	req.GroupName, err = p.getString(downstream, GroupNameMaxLen)
	if err != nil {
		return
	}
	var status int
	var recvBuf []byte
	recvBuf, status, err = p.call(&req, nil)
	if err != nil {
		return
	}
	h = Header{
		Status: status,
		PkgLen: len(recvBuf),
	}
	err = struc.Pack(downstream, &h)
	if status != 0 || err != nil {
		return
	}
	_, err = downstream.Write(recvBuf[:GroupNameMaxLen])
	if err != nil {
		return
	}
	b := bytes.NewReader(recvBuf[GroupNameMaxLen:]) // skip unused GroupName
	for b.Len() > 1 && err == nil {
		server := &StorageServer{}
		err = struc.Unpack(b, server)
		if err != nil {
			return
		}
		server.IpAddr = p.getDownStreamIp(strings.TrimRight(server.IpAddr, "\x00"))
		err = struc.Pack(downstream, server)
	}
	if err == nil {
		_, err = downstream.Write(recvBuf[len(recvBuf)-1:])
	}
	return
}

func (p *TrackerProxy) serviceQueryStoreWithoutGroup(downstream net.Conn, h Header) (err error) {
	var status int
	var recvBuf []byte
	recvBuf, status, err = p.call(&h, nil)
	if err != nil {
		return
	}
	h = Header{
		Status: status,
		PkgLen: len(recvBuf),
	}
	err = struc.Pack(downstream, &h)
	if status != 0 || err != nil {
		return
	}
	_, err = downstream.Write(recvBuf[:GroupNameMaxLen])
	b := bytes.NewReader(recvBuf[GroupNameMaxLen:]) // skip unused GroupName
	for b.Len() > 1 && err == nil {
		server := &StorageServer{}
		err = struc.Unpack(b, server)
		if err != nil {
			return
		}
		server.IpAddr = p.getDownStreamIp(strings.TrimRight(server.IpAddr, "\x00"))
		err = struc.Pack(downstream, server)
	}
	if err == nil {
		_, err = downstream.Write(recvBuf[len(recvBuf)-1:])
	}
	return
}

func (p *TrackerProxy) serviceQueryWithGroupNameFileName(downstream net.Conn, h Header) (err error) {
	req := FileObject{Header: h}
	req.GroupName, err = p.getString(downstream, GroupNameMaxLen)
	if err != nil {
		return
	}
	req.FileName, err = p.getString(downstream, h.PkgLen-GroupNameMaxLen)
	if err != nil {
		return
	}
	var recvBuf []byte
	var status int
	recvBuf, status, err = p.call(&req, nil)
	h = Header{
		Status: status,
		PkgLen: len(recvBuf),
	}
	if err != nil {
		return
	}
	err = struc.Pack(downstream, &h)
	if status != 0 || err != nil {
		return
	}
	idx := GroupNameMaxLen //skip
	host := p.getDownStreamIp(strings.TrimRight(string(recvBuf[idx:idx+IpAddressSize-1]), "\x00"))
	idx += IpAddressSize - 1
	buf := make([]byte, IpAddressSize-1)
	copy(buf, []byte(host))
	_, err = downstream.Write(recvBuf[:GroupNameMaxLen])
	if err != nil {
		return
	}
	_, err = downstream.Write(buf)
	if err != nil {
		return
	}
	_, err = downstream.Write(recvBuf[idx:])
	return
}

func (p *TrackerProxy) serviceQueryFetchAll(downstream net.Conn, h Header) (err error) {
	req := FileObject{Header: h}
	req.GroupName, err = p.getString(downstream, GroupNameMaxLen)
	if err != nil {
		return
	}
	req.FileName, err = p.getString(downstream, h.PkgLen-GroupNameMaxLen)
	if err != nil {
		return
	}
	var status int
	var recvBuf []byte
	recvBuf, status, err = p.call(&req, nil)
	if err != nil {
		return
	}
	//FETCH只返回一个服务
	h = Header{
		Status: status,
		PkgLen: len(recvBuf),
	}
	err = struc.Pack(downstream, &h)
	if status != 0 || err != nil {
		return
	}
	_, err = downstream.Write(recvBuf[:GroupNameMaxLen])
	if err != nil {
		return
	}
	ipBuf := make([]byte, IpAddressSize-1)
	idx := GroupNameMaxLen
	host := p.getDownStreamIp(strings.TrimRight(string(recvBuf[idx:idx+IpAddressSize-1]), "\x00"))
	copy(ipBuf, []byte(host))
	_, err = downstream.Write(ipBuf)
	if err != nil {
		return
	}
	idx += IpAddressSize - 1
	_, err = downstream.Write(recvBuf[idx : idx+PkgLenSize])
	idx += PkgLenSize
	for ; idx < len(recvBuf); idx += IpAddressSize - 1 {
		host = p.getDownStreamIp(strings.TrimRight(string(recvBuf[idx:idx+IpAddressSize-1]), "\x00"))
		copy(ipBuf, []byte(host))
		_, err = downstream.Write(ipBuf)
		if err != nil {
			return
		}
	}
	return
}
