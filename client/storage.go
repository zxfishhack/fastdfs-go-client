package client

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/lunixbochs/struc"
	. "github.com/zxfishhack/fastdfs-go-client/proto"
	"github.com/zxfishhack/fastdfs-go-client/util"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

const fdfsBase64Str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

var fdfsBase64 *base64.Encoding

func init() {
	fdfsBase64 = base64.NewEncoding(fdfsBase64Str).WithPadding('.')
}

type reader struct {
	io.Reader
	len int
}

func (r *reader) Len() int {
	return r.len
}

type StorageClient struct {
	tracker *TrackerClient
}

func NewStorageClient(client *TrackerClient) (c *StorageClient) {
	return &StorageClient{
		tracker: client,
	}
}

func (c *StorageClient) Upload(groupName, fileExtName string, appId, length int, r io.Reader, metas []*MetaData) (*FileResult, error) {
	return c.upload(StorageProtoCmdUploadFile, groupName, "", "", fileExtName, appId, length, r, metas)
}

func (c *StorageClient) UploadAppender(groupName, fileExtName string, appId, length int, r io.Reader, metas []*MetaData) (*FileResult, error) {
	return c.upload(StorageProtoCmdUploadAppenderFile, groupName, "", "", fileExtName, appId, length, r, metas)
}

func (c *StorageClient) UploadSlaveWithFileId(fileId, prefixName, fileExtName string, length int, r io.Reader, metas []*MetaData) (*FileResult, error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		return nil, util.ErrParam
	}
	return c.UploadSlave(ps[0], ps[1], prefixName, fileExtName, length, r, metas)
}

func (c *StorageClient) UploadSlave(groupName, masterFileName, prefixName, fileExtName string, length int, r io.Reader, metas []*MetaData) (*FileResult, error) {
	return c.upload(StorageProtoCmdUploadSlaveFile, groupName, masterFileName, prefixName, fileExtName, 0, length, r, metas)
}

// 使用Timed TRUNK时，以下接口无法使用
func (c *StorageClient) CreateLink(masterFileName, srcFileName, srcFileSig, groupName, prefixName, fileExtName string) (res *FileResult, err error) {
	req := &CreateLinkFile{
		Header: Header{
			PkgLen: PkgLenSize*3 + GroupNameMaxLen + FilePrefixMaxLen + FileExtNameMaxLen + len(masterFileName) + len(srcFileName) + len(srcFileSig),
			Cmd:    StorageProtoCmdCreateLink,
		},
		GroupName:      groupName,
		PrefixName:     prefixName,
		MasterFileName: masterFileName,
		SrcFileName:    srcFileName,
		SrcFileSig:     srcFileSig,
	}
	var sc *storageClient
	var addrs []string
	addrs, err = c.tracker.QueryStorageUpdate(groupName, srcFileName)
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	defer sc.Close()
	var recvBuf []byte
	var status int
	recvBuf, status, err = sc.call(req, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	res = &FileResult{
		GroupName:      strings.TrimRight(string(recvBuf[:GroupNameMaxLen]), "\x00"),
		RemoteFileName: string(recvBuf[GroupNameMaxLen:]),
	}
	return
}

func (c *StorageClient) FileInfoWithFileId(fileId string, fromServer bool) (fi *FileInfo, err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.FileInfo(ps[0], ps[1], fromServer)
}

func (c *StorageClient) FileInfo(groupName, fileName string, fromServer bool) (fi *FileInfo, err error) {
	if len(fileName) < LogicFilePathLen {
		err = ErrLength
		return
	}
	var buf []byte
	//~,~ auto padding...
	fiStr := fileName[LogicFilePathLen:LogicFilePathLen+FilenameBase64Length] + "."
	buf, err = fdfsBase64.DecodeString(fiStr)
	if err != nil {
		return
	}
	bfi := &Base64Info{}
	err = struc.Unpack(bytes.NewReader(buf), bfi)
	if err != nil {
		return
	}
	fi = &FileInfo{
		CreateTime: time.Unix(bfi.Timestamp, 0),
		Crc32:      bfi.Crc32,
		FileSize:   bfi.FileSize,
	}
	if GetServerIdType(bfi.IpOrId) == IdTypeServerId {
		log.Print("ServerId not support yet")
	} else {
		ip := &IpAddr{}
		_ = struc.Unpack(bytes.NewBuffer(buf), ip)
		fi.SourceIpAddr = ip.String()
	}
	if IsSlaveFile(fileName, fi.FileSize) || IsAppenderFile(fi.FileSize) || (fi.SourceIpAddr == "" && fromServer) {
		if fromServer {
			req := &FileObject{
				Header: Header{
					PkgLen: GroupNameMaxLen + len(fileName),
					Cmd:    StorageProtoCmdQueryFileInfo,
				},
				GroupName: groupName,
				FileName:  fileName,
			}
			sfi := &ServerFileInfo{}
			var status int
			var sc *storageClient
			var addrs []string
			addrs, err = c.tracker.QueryStorageFetch(groupName, fileName)
			if err != nil {
				return
			}
			sc, err = newStorageClient(strings.Join(addrs, ","))
			if sc == nil {
				log.Fatal("newStorageClient failed.")
			}
			defer sc.Close()
			status, err = sc.callWithInterface(req, nil, sfi)
			if err != nil {
				return
			}
			if status != 0 {
				err = util.GetErr(status)
				return
			}
			fi.FileSize = sfi.FileSize
			fi.SourceIpAddr = strings.TrimRight(sfi.IpAddr, "\x00")
			fi.Crc32 = sfi.Crc32
			fi.CreateTime = time.Unix(sfi.Timestamp, 0)
		} else {
			fi.FileSize = -1
		}
	} else {
		if fi.FileSize>>63 != 0 {
			fi.FileSize &= 0xFFFFFFFF
		} else if IsTrunkFile(fi.FileSize) {
			fi.FileSize &= 0xFFFFFFFF
		}
	}
	return
}

func (c *StorageClient) AppendWithFileId(fileId string, length int, r io.Reader) (err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.Append(ps[0], ps[1], length, r)
}

func (c *StorageClient) Append(groupName, fileName string, length int, r io.Reader) (err error) {
	var sc *storageClient
	reader := &reader{Reader: r, len: length}
	var addrs []string
	addrs, err = c.tracker.QueryStorageUpdate(groupName, fileName)
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	defer sc.Close()
	req := &AppendFile{
		Header: Header{
			PkgLen: PkgLenSize*2 + len(fileName) + length,
			Cmd:    StorageProtoCmdAppendFile,
		},
		Size:     length,
		FileName: fileName,
	}
	var status int
	_, status, err = sc.call(req, reader)
	if err == nil && status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *StorageClient) TruncateWithFileId(fileId string, offset int) (err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.Truncate(ps[0], ps[1], offset)
}

func (c *StorageClient) Truncate(groupName, fileName string, offset int) (err error) {
	var sc *storageClient
	var addrs []string
	addrs, err = c.tracker.QueryStorageUpdate(groupName, fileName)
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	defer sc.Close()
	req := &TruncateFile{
		Header: Header{
			PkgLen: PkgLenSize*2 + len(fileName),
			Cmd:    StorageProtoCmdTruncateFile,
		},
		Offset:   offset,
		FileName: fileName,
	}
	var status int
	_, status, err = sc.call(req, nil)
	if err == nil && status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *StorageClient) ModifyWithFileId(fileId string, offset, length int, r io.Reader) (err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.ModifyFile(ps[0], ps[1], offset, length, r)
}

func (c *StorageClient) ModifyFile(groupName, fileName string, offset, length int, r io.Reader) (err error) {
	reader := &reader{Reader: r, len: length}
	var sc *storageClient
	var addrs []string
	addrs, err = c.tracker.QueryStorageUpdate(groupName, fileName)
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	defer sc.Close()
	req := &ModifyFile{
		Header: Header{
			PkgLen: PkgLenSize*3 + len(fileName) + length,
			Cmd:    StorageProtoCmdModifyFile,
		},
		Offset:   offset,
		Length:   length,
		FileName: fileName,
	}
	var status int
	_, status, err = sc.call(req, reader)
	if err == nil && status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *StorageClient) DownloadWithFileId(fileId string, offset, length int, w io.Writer) (err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.Download(ps[0], ps[1], offset, length, w)
}

func (c *StorageClient) Download(groupName, fileName string, offset, length int, w io.Writer) (err error) {
	req := &FileObjectOffsetLength{
		Header: Header{
			PkgLen: GroupNameMaxLen + len(fileName) + PkgLenSize*2,
			Cmd:    StorageProtoCmdDownloadFile,
		},
		Offset:    offset,
		Length:    length,
		GroupName: groupName,
		FileName:  fileName,
	}
	var sc *storageClient
	var status int
	var addrs []string
	addrs, err = c.tracker.QueryStorageFetch(groupName, fileName)
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	status, err = sc.callWithWriter(req, nil, w)
	if err == nil && status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *StorageClient) SetMetaWithFileId(fileId string, metas []*MetaData) (err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.SetMeta(ps[0], ps[1], metas)
}

func (c *StorageClient) SetMeta(groupName, fileName string, metas []*MetaData) (err error) {
	return c.setMeta(groupName, fileName, metas, StorageSetMetadataFlagOverwriteStr)
}

func (c *StorageClient) MergeMetaWithFileId(fileId string, metas []*MetaData) (err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.MergeMeta(ps[0], ps[1], metas)
}

func (c *StorageClient) MergeMeta(groupName, fileName string, metas []*MetaData) (err error) {
	return c.setMeta(groupName, fileName, metas, StorageSetMetadataFlagMergeStr)
}

func (c *StorageClient) setMeta(groupName, fileName string, metas []*MetaData, flag string) (err error) {
	var sc *storageClient
	var addrs []string
	addrs, err = c.tracker.QueryStorageUpdate(groupName, fileName)
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	req := &SetMeta{
		Header: Header{
			PkgLen: PkgLenSize*2 + 1 + GroupNameMaxLen + len(fileName),
			Cmd:    StorageProtoCmdSetMetadata,
		},
		Flag:      flag,
		GroupName: groupName,
		FileName:  fileName,
	}
	var pm []string
	for _, meta := range metas {
		pm = append(pm, meta.String())
	}
	req.PackMeta = strings.Join(pm, RecordSeperator)
	req.PkgLen += len(req.PackMeta)
	var status int
	_, status, err = sc.call(req, nil)
	if err == nil && status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *StorageClient) GetMetaWithFileId(fileId string) (metas []*MetaData, err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.GetMeta(ps[0], ps[1])
}

func (c *StorageClient) GetMeta(groupName, fileName string) (metas []*MetaData, err error) {
	req := &FileObject{
		Header: Header{
			PkgLen: GroupNameMaxLen + len(fileName),
			Cmd:    StorageProtoCmdGetMetadata,
		},
		GroupName: groupName,
		FileName:  fileName,
	}
	var sc *storageClient
	var recvBuff []byte
	var status int
	var addrs []string
	addrs, err = c.tracker.QueryStorageUpdate(groupName, fileName)
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	recvBuff, status, err = sc.call(req, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	ps := strings.Split(string(recvBuff), RecordSeperator)
	for _, meta := range ps {
		nv := strings.Split(meta, FieldSeperator)
		if len(nv) != 2 {
			continue
		}
		metas = append(metas, &MetaData{
			Name:  nv[0],
			Value: nv[1],
		})
	}
	return
}

func (c *StorageClient) DeleteWithFileId(fileId string) (err error) {
	ps := strings.SplitN(fileId, "/", 2)
	if len(ps) != 2 {
		err = util.ErrParam
		return
	}
	return c.Delete(ps[0], ps[1])
}

func (c *StorageClient) Delete(groupName, fileName string) (err error) {
	req := &FileObject{
		Header: Header{
			PkgLen: GroupNameMaxLen + len(fileName),
			Cmd:    StorageProtoCmdDeleteFile,
		},
		GroupName: groupName,
		FileName:  fileName,
	}
	var sc *storageClient
	var status int
	var addrs []string
	addrs, err = c.tracker.QueryStorageUpdate(groupName, fileName)
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	_, status, err = sc.call(req, nil)
	if err == nil && status != 0 {
		err = util.GetErr(status)
	}
	return
}

func (c *StorageClient) GetTimedStorageStatus() (res *TimedStorageStatus, groups []*GroupStat, err error) {
	groups, err = c.tracker.ListGroups()
	if err != nil {
		return
	}
	res = &TimedStorageStatus{}
	g, _ := errgroup.WithContext(c.tracker.ctx)
	var mtx sync.Mutex

	for _, group := range groups {
		groupSt := &GroupStatus{GroupName: group.GroupName}
		res.Groups = append(res.Groups, groupSt)
		var sts []*StorageStat
		sts, err = c.tracker.ListServers(group.GroupName, "")
		if err != nil {
			return
		}
		for _, st := range sts {
			addr := fmt.Sprintf("%s:%d", st.IpAddr, st.StoragePort)
			if st.Status != StorageStatusActive {
				mtx.Lock()
				groupSt.Servers = append(groupSt.Servers, &ServerStatus{
					Id:     st.Id,
					IpAddr: st.IpAddr,
					Active: false,
				})
				mtx.Unlock()
				continue
			}
			g.Go(func() error {
				st, e := c.getServerStatus(st.Id, addr)
				if e != nil {
					log.Printf("get Storage Status @ %s failed %v.", addr, err)
				}
				if st != nil {
					mtx.Lock()
					groupSt.Servers = append(groupSt.Servers, st)
					mtx.Unlock()
				}
				return e
			})
		}
	}
	err = g.Wait()
	return
}

func (c *StorageClient) upload(cmd int, groupName, masterFileName, prefixName, fileExtName string,
	appId, length int, r io.Reader, metas []*MetaData) (res *FileResult, err error) {
	storePathIndex := -1
	reader := &reader{Reader: r, len: length}
	uploadSlave := len(groupName) > 0 && len(masterFileName) > 0
	var sc *storageClient
	var addrs []string
	if uploadSlave {
		addrs, err = c.tracker.QueryStorageUpdate(groupName, masterFileName)
	} else {
		addrs, storePathIndex, err = c.tracker.QueryStorageStore(groupName)
	}
	if err != nil {
		return
	}
	sc, err = newStorageClient(strings.Join(addrs, ","))
	if err != nil {
		return
	}
	defer sc.Close()
	var req interface{}
	if uploadSlave {
		req = &UploadSlave{
			Header: Header{
				PkgLen: PkgLenSize + PkgLenSize + FilePrefixMaxLen + FileExtNameMaxLen + len(masterFileName) + length,
				Cmd:    cmd,
			},
			FileSize:       length,
			PrefixName:     prefixName,
			FileExtName:    fileExtName,
			MasterFileName: masterFileName,
		}
	} else {
		req = &UploadFile{
			Header: Header{
				PkgLen: 1 + PkgLenSize + FileExtNameMaxLen + length + AppIdSize,
				Cmd:    cmd,
			},
			AppId:          appId,
			FileSize:       length,
			StorePathIndex: storePathIndex,
			FileExtName:    fileExtName,
		}
	}

	var status int
	var recvBuf []byte
	recvBuf, status, err = sc.call(req, reader)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	res = &FileResult{
		GroupName:      strings.TrimRight(string(recvBuf[:GroupNameMaxLen]), "\x00"),
		RemoteFileName: string(recvBuf[GroupNameMaxLen:]),
	}
	if len(metas) > 0 {
		err = c.SetMeta(res.GroupName, res.RemoteFileName, metas)
		if err != nil {
			_ = c.Delete(res.GroupName, res.RemoteFileName)
			res = &FileResult{}
		}
	}
	return
}

func (c *StorageClient) getServerStatus(id, addr string) (res *ServerStatus, err error) {
	h := Header{
		Cmd: StorageProtoCmdGetStorageStatus,
	}
	var sc *storageClient
	sc, err = newStorageClient(addr)
	if err != nil {
		return
	}
	defer sc.Close()
	var recvBuf []byte
	var status int
	recvBuf, status, err = sc.call(&h, nil)
	if err != nil {
		return
	}
	if status != 0 {
		err = util.GetErr(status)
		return
	}
	/** output format
	4 bytes 存储路径数量
	   1 byte 存储路径是否可用
	   4 bytes 业务ID数量
	       4 bytes 业务ID
	       4 bytes 日期数量
	           4 bytes 日期
	**/
	res = &ServerStatus{Id: id, IpAddr: addr, Active: true}
	reader := bytes.NewReader(recvBuf)
	var storeCount int32
	err = binary.Read(reader, binary.BigEndian, &storeCount)
	if err != nil {
		return
	}
	for i := 0; i < int(storeCount); i++ {
		storeSt := &StorePathStatus{}
		res.StorePaths = append(res.StorePaths, storeSt)
		var valid byte
		var appIdCount int32
		err = binary.Read(reader, binary.BigEndian, &valid)
		if err != nil {
			return
		}
		storeSt.Valid = valid != 0
		if valid == 0 {
			continue
		}
		err = binary.Read(reader, binary.BigEndian, &appIdCount)
		if err != nil {
			return
		}
		if appIdCount <= 0 {
			continue
		}
		for j := 0; j < int(appIdCount); j++ {
			var appId int32
			err = binary.Read(reader, binary.BigEndian, &appId)
			if err != nil {
				return
			}
			appIdSt := &AppIdStatus{AppId: fmt.Sprintf("%08X", appId)}
			storeSt.AppIdStatus = append(storeSt.AppIdStatus, appIdSt)
			var dayCount int32
			err = binary.Read(reader, binary.BigEndian, &dayCount)
			if err != nil {
				return
			}
			for k := 0; k < int(dayCount); k++ {
				var dayId int32
				err = binary.Read(reader, binary.BigEndian, &dayId)
				if err != nil {
					return
				}
				appIdSt.DayId = append(appIdSt.DayId, fmt.Sprintf("%08X", dayId))
			}
			sort.Strings(appIdSt.DayId)
		}
	}
	return
}
