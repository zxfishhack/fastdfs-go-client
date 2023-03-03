package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/lunixbochs/struc"
	. "github.com/zxfishhack/fastdfs-go-client/proto"
	"github.com/zxfishhack/fastdfs-go-client/util"
	"strings"
)

func (c *TrackerClient) QueryStorageStoreOne(groupName string) (addrs []string, storePathIndex int, err error) {
	return c.queryStorageCheckGroupName([]int{
		TrackerProtoCmdServiceQueryStoreWithoutGroupOne,
		TrackerProtoCmdServiceQueryStoreWithGroupOne,
	}, groupName)
}

func (c *TrackerClient) QueryStorageStore(groupName string) (addrs []string, storePathIndex int, err error) {
	return c.queryStorageCheckGroupName([]int{
		TrackerProtoCmdServiceQueryStoreWithoutGroupAll,
		TrackerProtoCmdServiceQueryStoreWithGroupAll,
	}, groupName)
}

func (c *TrackerClient) QueryStorageUpdate(groupName string, filename string) ([]string, error) {
	return c.queryStorageWithGroupNameFileName(TrackerProtoCmdServiceQueryUpdate, groupName, filename)
}

func (c *TrackerClient) QueryStorageFetch(groupName string, filename string) ([]string, error) {
	return c.queryStorageWithGroupNameFileName(TrackerProtoCmdServiceQueryFetchOne, groupName, filename)
}

func (c *TrackerClient) QueryStorageList(groupName string, filename string) (addrs []string, returnGroupName string, err error) {
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	req := &FileObject{
		Header: Header{
			PkgLen: GroupNameMaxLen + len(filename),
			Cmd:    TrackerProtoCmdServiceQueryFetchAll,
		},
		GroupName: groupName,
		FileName:  filename,
	}
	var recvBuff []byte
	var status int
	recvBuff, status, err = c.call(req, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	returnGroupName = strings.TrimRight(string(recvBuff[:GroupNameMaxLen]), "\x00")
	var port int64
	idx := GroupNameMaxLen
	host := strings.TrimRight(string(recvBuff[idx:idx+IpAddressSize-1]), "\x00")
	idx += IpAddressSize - 1
	err = binary.Read(bytes.NewReader(recvBuff[idx:idx+PkgLenSize]), binary.BigEndian, &port)
	idx += PkgLenSize
	if err != nil {
		return
	}
	addrs = []string{fmt.Sprintf("%s:%d", host, port)}
	for ; idx < len(recvBuff); idx += IpAddressSize - 1 {
		host = strings.TrimRight(string(recvBuff[idx:idx+IpAddressSize-1]), "\x00")
		addrs = append(addrs, fmt.Sprintf("%s:%d", host, port))
	}
	return
}

// 当groupName不存在时，使用cmd[0]，当groupName存在时，使用cmd[1]
func (c *TrackerClient) queryStorageCheckGroupName(cmd []int, groupName string) (addrs []string, storePathIndex int, err error) {
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	var req interface{}
	if groupName == "" {
		req = &Header{
			Cmd: cmd[0],
		}
	} else {
		req = &FileObject{
			Header: Header{
				PkgLen: GroupNameMaxLen,
				Cmd:    cmd[1],
			},
			GroupName: groupName,
		}
	}
	var recvBuff []byte
	var status int
	recvBuff, status, err = c.call(req, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	b := bytes.NewReader(recvBuff[GroupNameMaxLen:]) // skip unused GroupName
	for b.Len() > 1 {
		server := &StorageServer{}
		err = struc.Unpack(b, server)
		if err != nil {
			return
		}
		server.IpAddr = strings.TrimRight(server.IpAddr, "\x00")
		addrs = append(addrs, fmt.Sprintf("%s:%d", server.IpAddr, server.Port))
	}
	storePathIndex = int(recvBuff[len(recvBuff)-1])
	return
}

func (c *TrackerClient) queryStorageWithGroupNameFileName(cmd int, groupName string, filename string) (addrs []string, err error) {
	if len(groupName) > GroupNameMaxLen {
		groupName = groupName[:GroupNameMaxLen]
	}
	req := &FileObject{
		Header: Header{
			PkgLen: GroupNameMaxLen + len(filename),
			Cmd:    cmd,
		},
		GroupName: groupName,
		FileName:  filename,
	}
	var recvBuff []byte
	var status int
	recvBuff, status, err = c.call(req, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	var port int64
	idx := GroupNameMaxLen //skip
	host := strings.TrimRight(string(recvBuff[idx:idx+IpAddressSize-1]), "\x00")
	idx += IpAddressSize - 1
	err = binary.Read(bytes.NewReader(recvBuff[idx:idx+8]), binary.BigEndian, &port)
	idx += 8
	if err != nil {
		return
	}
	addrs = append(addrs, fmt.Sprintf("%s:%d", host, port))
	return
}
