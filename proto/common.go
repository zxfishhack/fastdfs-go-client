package proto

import (
	"fmt"
	"time"
)

const (
	PkgLenSize      = 8
	AppIdSize       = 4
	IpAddressSize   = 16
	GroupNameMaxLen = 16
	DirSize         = 8

	FileExtNameMaxLen    = 6
	FilePrefixMaxLen     = 16
	LogicFilePathLen     = 22
	TrueFilePathLen      = 18
	FilenameBase64Length = 27
	TrunkFileInfoLen     = 16

	StorageIdMaxSize = 16

	IdTypeServerId  = 1
	IdTypeIpAddress = 2
	MaxServerId     = (1 << 24) - 1

	NormalLogicFilenameLength = LogicFilePathLen + FilenameBase64Length + FileExtNameMaxLen + 1
	TrunkFilenameLength       = TrueFilePathLen + FilenameBase64Length + TrunkFileInfoLen + 1 + FileExtNameMaxLen
	TrunkLogicFilenameLength  = TrunkFilenameLength + LogicFilePathLen - TrueFilePathLen

	InfiniteFileSize  = 256 * 1024 * 1024 * 1024 * 1024 * 1024
	AppenderFileSize  = InfiniteFileSize
	TrunkFileMarkSize = 512 * 1024 * 1024 * 1024 * 1024 * 1024

	StorageStatusInit      = 0
	StorageStatusWaitSync  = 1
	StorageStatusSyncing   = 2
	StorageStatusIpChanged = 3
	StorageStatusDeleted   = 4
	StorageStatusOffline   = 5
	StorageStatusOnline    = 6
	StorageStatusActive    = 7
	StorageStatusRecovery  = 9
)

type FileObject struct {
	Header
	GroupName string `struc:"[16]byte"` //GroupNameMaxLen
	FileName  string
}

type FileObjectOffsetLength struct {
	Header
	Offset    int    `struc:"int64"`
	Length    int    `struc:"int64"`
	GroupName string `struc:"[16]byte"` //GroupNameMaxLen
	FileName  string
}

type Base64Info struct {
	IpOrId    int   `struc:"int32,little"`
	Timestamp int64 `struc:"int32"`
	FileSize  int   `struc:"int64"`
	Crc32     uint  `struc:"uint32"`
}

type IpAddr struct {
	P1 int `struc:"byte"`
	P2 int `struc:"byte"`
	P3 int `struc:"byte"`
	P4 int `struc:"byte"`
}

func (ip *IpAddr) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", ip.P1, ip.P2, ip.P3, ip.P4)
}

func GetServerIdType(id int) int {
	if id > 0 && id < MaxServerId {
		return IdTypeServerId
	}
	return IdTypeIpAddress
}

func IsAppenderFile(fileSize int) bool {
	return (fileSize & AppenderFileSize) != 0
}

func IsTrunkFile(fileSize int) bool {
	return (fileSize & TrunkFileMarkSize) != 0
}

func IsSlaveFile(fileName string, fileSize int) bool {
	return len(fileName) > TrunkLogicFilenameLength || (len(fileName) > NormalLogicFilenameLength && !IsTrunkFile(fileSize))
}

type FileInfo struct {
	CreateTime   time.Time
	Crc32        uint
	SourceId     int
	FileSize     int
	SourceIpAddr string
}

type ServerFileInfo struct {
	FileSize  int    `struc:"int64"`
	Timestamp int64  `struc:"int64"`
	Crc32     uint   `struc:"uint64"`
	IpAddr    string `struc:"[16]byte"` //IpAddressSize
}

func StorageStatus(status int) string {
	switch status {
	case StorageStatusInit:
		return "INIT"
	case StorageStatusWaitSync:
		return "WAIT_SYNC"
	case StorageStatusSyncing:
		return "SYNCING"
	case StorageStatusOffline:
		return "OFFLINE"
	case StorageStatusOnline:
		return "ONLINE"
	case StorageStatusDeleted:
		return "DELETED"
	case StorageStatusIpChanged:
		return "IP_CHANGED"
	case StorageStatusActive:
		return "ACTIVE"
	case StorageStatusRecovery:
		return "RECOVERY"
	}
	return fmt.Sprintf("UNKNOWN STATUS[%d]", status)
}
