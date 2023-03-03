package client

import (
	"bytes"
	"context"
	"github.com/lunixbochs/struc"
	. "github.com/zxfishhack/fastdfs-go-client/proto"
	"github.com/zxfishhack/fastdfs-go-client/util"
	"gopkg.in/fatih/pool.v2"
	"net"
	"strings"
)

type TrackerClient struct {
	Client
	ctx context.Context
}

func NewTrackerClient(addrs string) (*TrackerClient, error) {
	return NewTrackerClientWithContext(context.Background(), addrs)
}

func NewTrackerClientWithContext(ctx context.Context, addrs string) (c *TrackerClient, err error) {
	c = &TrackerClient{ctx: ctx}
	err = c.Init(ctx, addrs)
	return
}

func (c *TrackerClient) Close() {
}

func (c *TrackerClient) ListServers(groupName, storageId string) (storages []*StorageStat, err error) {
	var status int
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	if len(storageId) > StorageIdMaxSize {
		storageId = storageId[:StorageIdMaxSize]
	}
	req := &ListServersReq{
		Header: Header{
			PkgLen: GroupNameMaxLen + len(storageId),
			Cmd:    TrackerProtoCmdServerListStorage,
		},
		StorageIdBody: StorageIdBody{
			GroupName: groupName,
			StorageId: storageId,
		},
	}
	var recvBuf []byte
	recvBuf, status, err = c.call(req, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	b := bytes.NewReader(recvBuf)
	for b.Len() > 0 {
		storage := &StorageStat{}
		err = struc.Unpack(b, storage)
		if err != nil {
			return
		}
		storage.Id = strings.TrimRight(storage.Id, "\x00")
		storage.DomainName = strings.TrimRight(storage.DomainName, "\x00")
		storage.IpAddr = strings.TrimRight(storage.IpAddr, "\x00")
		storage.SrcId = strings.TrimRight(storage.SrcId, "\x00")
		storage.Version = strings.TrimRight(storage.Version, "\x00")
		storages = append(storages, storage)
	}
	return
}

func (c *TrackerClient) ListGroups() (groups []*GroupStat, err error) {
	var status int
	req := &ListGroupsReq{
		Header: Header{
			Cmd: TrackerProtoCmdServerListAllGroups,
		},
	}
	var recvBuf []byte
	recvBuf, status, err = c.call(req, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	b := bytes.NewReader(recvBuf)
	for b.Len() > 0 {
		group := &GroupStat{}
		err = struc.Unpack(b, group)
		if err != nil {
			return
		}
		group.GroupName = strings.TrimRight(group.GroupName, "\x00")
		groups = append(groups, group)
	}
	return
}

func (c *TrackerClient) ListOneGroup(groupName string) (group *GroupStat, err error) {
	var status int
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	req := &ListOneGroupReq{
		Header: Header{
			PkgLen: GroupNameMaxLen,
			Cmd:    TrackerProtoCmdServerListOneGroup,
		},
		GroupNameBody: GroupNameBody{
			GroupName: groupName,
		},
	}
	group = &GroupStat{}
	status, err = c.callWithInterface(req, nil, group)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	group.GroupName = strings.TrimRight(group.GroupName, "\x00")
	return
}

func (c *TrackerClient) DeleteStorage(groupName, storageId string) (err error) {
	var status int
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	if len(storageId) > StorageIdMaxSize {
		storageId = storageId[:StorageIdMaxSize]
	}
	req := &DeleteStorageReq{
		Header: Header{
			Cmd:    TrackerProtoCmdServerDeleteStorage,
			PkgLen: GroupNameMaxLen + len(storageId),
		},
		StorageIdBody: StorageIdBody{
			GroupName: groupName,
			StorageId: storageId,
		},
	}
	_, status, err = c.call(req, nil)
	if status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *TrackerClient) DeleteGroup(groupName string) (err error) {
	var status int
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	req := &DeleteGroupReq{
		Header: Header{
			Cmd:    TrackerProtoCmdServerDeleteGroup,
			PkgLen: GroupNameMaxLen,
		},
		GroupNameBody: GroupNameBody{
			GroupName: groupName,
		},
	}
	_, status, err = c.call(req, nil)
	if status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *TrackerClient) DeleteDir(appId int, dir string) (err error) {
	var status int
	req := &DeleteDirReq{
		Header: Header{
			Cmd:    TrackerProtoCmdAddDel,
			PkgLen: DirSize + AppIdSize,
		},
		DeleteDirBody: DeleteDirBody{
			AppId: appId,
			Dir:   dir,
		},
	}
	_, status, err = c.call(req, nil)
	if status == 17 {
		status = 0
	}
	if status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *TrackerClient) QueryDeleteDirStatus(appId int, dir string) (progress int, err error) {
	var status int
	req := &DeleteDirReq{
		Header: Header{
			Cmd:    TrackerProtoCmdQueryStatusDel,
			PkgLen: DirSize + AppIdSize,
		},
		DeleteDirBody: DeleteDirBody{
			AppId: appId,
			Dir:   dir,
		},
	}
	var prog int32
	status, err = c.callWithInterface(req, nil, &prog)
	if status != 0 {
		err = util.GetErr(status)
	}
	progress = int(prog)
	return
}

func (c *TrackerClient) SetTrunkServer(groupName, storageId string) (newTrunkServerId string, err error) {
	var nc net.Conn
	var status int
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	if len(storageId) > StorageIdMaxSize {
		storageId = storageId[:StorageIdMaxSize]
	}
	req := &SetTrunkServerReq{
		Header: Header{
			Cmd:    TrackerProtoCmdServerSetTrunkServer,
			PkgLen: GroupNameMaxLen + len(storageId),
		},
		StorageIdBody: StorageIdBody{
			GroupName: groupName,
			StorageId: storageId,
		},
	}
	for _, p := range c.pools {
		nc, err = p.Get()
		if err != nil {
			continue
		}
		var recvBuf []byte
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
			newTrunkServerId = storageId
			_ = nc.Close()
			return
		}
		if status != 0 {
			err = util.GetErr(status)
			_ = nc.Close()
			return
		}
		newTrunkServerId = strings.TrimRight(string(recvBuf), "\x00")
		_ = nc.Close()
		return
	}
	return
}

func (c *TrackerClient) GetStorageStatus(groupName, ipAddr string) (brief *StorageBrief, err error) {
	var status int
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	if len(ipAddr) > IpAddressSize {
		ipAddr = ipAddr[:IpAddressSize]
	}
	req := &GetStorageStatusReq{
		Header: Header{
			Cmd:    TrackerProtoCmdStorageGetStatus,
			PkgLen: GroupNameMaxLen + len(ipAddr),
		},
		StorageIdBody: StorageIdBody{
			GroupName: groupName,
			StorageId: ipAddr,
		},
	}
	brief = &StorageBrief{}
	status, err = c.callWithInterface(req, nil, brief)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	return
}

func (c *TrackerClient) GetStorageId(groupName, ipAddr string) (storageId string, err error) {
	var status int
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	if len(ipAddr) > IpAddressSize {
		ipAddr = ipAddr[:IpAddressSize]
	}
	req := &GetServerIdReq{
		Header: Header{
			Cmd:    TrackerProtoCmdStorageGetServerId,
			PkgLen: GroupNameMaxLen + len(ipAddr),
		},
		StorageIdBody: StorageIdBody{
			GroupName: groupName,
			StorageId: ipAddr,
		},
	}
	var recvBuf []byte
	recvBuf, status, err = c.call(req, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	storageId = strings.TrimRight(string(recvBuf), "\x00")
	return
}

func (c *TrackerClient) GetStorageMaxStatus(groupName, ipAddr string) (storageId string, status int, err error) {
	var brief *StorageBrief
	brief, err = c.GetStorageStatus(groupName, ipAddr)
	if brief != nil {
		storageId = brief.Id
		status = brief.Status
	}
	return
}
