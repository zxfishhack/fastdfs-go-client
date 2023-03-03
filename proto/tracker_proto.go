package proto

import (
	"errors"
)

const (
	TrackerProtoCmdStorageGetStatus   = 71 //get storage status from tracker
	TrackerProtoCmdStorageGetServerId = 70 //get storage server id from tracker

	TrackerProtoCmdServerListOneGroup               = 90
	TrackerProtoCmdServerListAllGroups              = 91
	TrackerProtoCmdServerListStorage                = 92
	TrackerProtoCmdServerDeleteStorage              = 93
	TrackerProtoCmdServerSetTrunkServer             = 94
	TrackerProtoCmdServiceQueryStoreWithoutGroupOne = 101
	TrackerProtoCmdServiceQueryFetchOne             = 102
	TrackerProtoCmdServiceQueryUpdate               = 103
	TrackerProtoCmdServiceQueryStoreWithGroupOne    = 104
	TrackerProtoCmdServiceQueryFetchAll             = 105
	TrackerProtoCmdServiceQueryStoreWithoutGroupAll = 106
	TrackerProtoCmdServiceQueryStoreWithGroupAll    = 107
	TrackerProtoCmdServerDeleteGroup                = 108

	TrackerProtoCmdAddDel         = 165
	TrackerProtoCmdQueryStatusDel = 166
)

var (
	ErrLength = errors.New("length not expect")
)

/*
typedef struct
{
	pkg_len int `struc:"int64"`  //body length, not including header
	cmd;    //command code
	status; //status code for response
} Header;
*/

type Header struct {
	PkgLen int `struc:"int64"`
	Cmd    int `struc:"byte"`
	Status int `struc:"byte"`
}

type ListGroupsReq struct {
	Header
}

type GroupNameBody struct {
	GroupName string `struc:"[16]byte"` //GroupNameMaxLen
}

type ListOneGroupReq struct {
	Header
	GroupNameBody
}

type StorageIdBody struct {
	GroupName string `struc:"[16]byte"` //GroupNameMaxLen
	StorageId string
}

type ListServersReq struct {
	Header
	StorageIdBody
}

type StorageServer struct {
	IpAddr string `struc:"[15]byte"` //IpAddressSize-1
	Port   int    `struc:"int64"`
}

type DeleteStorageReq struct {
	Header
	StorageIdBody
}

type DeleteGroupReq struct {
	Header
	GroupNameBody
}

type DeleteDirBody struct {
	AppId int    `struc:"int32"`
	Dir   string `struc:"[8]byte"`
}

type DeleteDirReq struct {
	Header
	DeleteDirBody
}

type SetTrunkServerReq struct {
	Header
	StorageIdBody
}

type GetStorageStatusReq struct {
	Header
	StorageIdBody
}

type GetServerIdReq struct {
	Header
	StorageIdBody
}

type GroupStat struct {
	GroupName          string `struc:"[17]byte"` //FDFS_GROUP_NAME_MAX_LEN + 1
	TotalMb            uint64 `struc:"uint64"`   //total disk storage in MB
	FreeMb             uint64 `struc:"uint64"`   //free disk storage in MB
	TrunkFreeMb        uint64 `struc:"uint64"`   //trunk free space in MB
	Count              int    `struc:"int64"`    //server count
	StoragePort        int    `struc:"int64"`
	StorageHttpPort    int    `struc:"int64"`
	ActiveCount        int    `struc:"int64"` //active server count
	CurrentWriteServer int    `struc:"int64"`
	StorePathCount     int    `struc:"int64"`
	SubdirCountPerPath int    `struc:"int64"`
	CurrentTrunkFileId int    `struc:"int64"`
}

type StorageStat struct {
	Status             int    `struc:"byte"`
	Id                 string `struc:"[16]byte"`  //FDFS_STORAGE_ID_MAX_SIZE
	IpAddr             string `struc:"[16]byte"`  //IP_ADDRESS_SIZE
	DomainName         string `struc:"[128]byte"` //FDFS_DOMAIN_NAME_MAX_SIZE
	SrcId              string `struc:"[16]byte"`  //FDFS_STORAGE_ID_MAX_SIZE
	Version            string `struc:"[6]byte"`   //FDFS_VERSION_SIZE
	JoinTime           int64  `struc:"int64"`
	UpTime             int64  `struc:"int64"`
	TotalMb            uint64 `struc:"uint64"`
	FreeMb             uint64 `struc:"uint64"`
	UploadPriority     int    `struc:"int64"`
	StorePathCount     int    `struc:"int64"`
	SubDirCountPerPath int    `struc:"int64"`
	CurrentWritePath   int    `struc:"int64"`
	StoragePort        int    `struc:"int64"`
	StorageHttpPort    int    `struc:"int64"`
	Detail             StorageStatDetail
	TrunkServer        bool `struc:"byte"`
}

type StorageBrief struct {
	Status int    `struc:"byte"`
	Port   int    `struc:"int32"`
	Id     string `struc:"[16]byte"`
	IpAddr string `struc:"[16]byte"`
}

type StorageStatDetail struct {
	AllocCount   int `struc:"int32"`
	CurrentCount int `struc:"int32"`
	MaxCount     int `struc:"int32"`

	TotalUploadCount       int    `struc:"int64"`
	SuccessUploadCount     int    `struc:"int64"`
	TotalAppendCount       int    `struc:"int64"`
	SuccessAppendCount     int    `struc:"int64"`
	TotalModifyCount       int    `struc:"int64"`
	SuccessModifyCount     int    `struc:"int64"`
	TotalTruncateCount     int    `struc:"int64"`
	SuccessTruncateCount   int    `struc:"int64"`
	TotalSetMetaCount      int    `struc:"int64"`
	SuccessSetMetaCount    int    `struc:"int64"`
	TotalDeleteCount       int    `struc:"int64"`
	SuccessDeleteCount     int    `struc:"int64"`
	TotalDownloadCount     int    `struc:"int64"`
	SuccessDownloadCount   int    `struc:"int64"`
	TotalGetMetaCount      int    `struc:"int64"`
	SuccessGetMetaCount    int    `struc:"int64"`
	TotalCreateLinkCount   int    `struc:"int64"`
	SuccessCreateLinkCount int    `struc:"int64"`
	TotalDeleteLinkCount   int    `struc:"int64"`
	SuccessDeleteLinkCount int    `struc:"int64"`
	TotalUploadBytes       uint64 `struc:"uint64"`
	SuccessUploadBytes     uint64 `struc:"uint64"`
	TotalAppendBytes       uint64 `struc:"uint64"`
	SuccessAppendBytes     uint64 `struc:"uint64"`
	TotalModifyBytes       uint64 `struc:"uint64"`
	SuccessModifyBytes     uint64 `struc:"uint64"`
	TotalDownloadBytes     uint64 `struc:"uint64"`
	SuccessDownloadBytes   uint64 `struc:"uint64"`
	TotalSyncInBytes       uint64 `struc:"uint64"`
	SuccessSyncInBytes     uint64 `struc:"uint64"`
	TotalSyncOutBytes      uint64 `struc:"uint64"`
	SuccessSyncOutBytes    uint64 `struc:"uint64"`
	TotalFileOpenCount     int    `struc:"int64"`
	SuccessFileOpenCount   int    `struc:"int64"`
	TotalFileReadCount     int    `struc:"int64"`
	SuccessFileReadCount   int    `struc:"int64"`
	TotalFileWriteCount    int    `struc:"int64"`
	SuccessFileWriteCount  int    `struc:"int64"`
	LastSourceUpdate       int64  `struc:"int64"`
	LastSyncUpdate         int64  `struc:"int64"`
	LastSyncedTimestamp    int64  `struc:"int64"`
	LastHeartBeatTime      int64  `struc:"int64"`
}
