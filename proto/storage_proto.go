package proto

const (
	StorageProtoCmdUploadFile         = 129
	StorageProtoCmdDeleteFile         = 12
	StorageProtoCmdSetMetadata        = 13
	StorageProtoCmdDownloadFile       = 14
	StorageProtoCmdGetMetadata        = 15
	StorageProtoCmdCreateLink         = 20
	StorageProtoCmdUploadSlaveFile    = 21
	StorageProtoCmdQueryFileInfo      = 22
	StorageProtoCmdUploadAppenderFile = 130 //create appender file
	StorageProtoCmdAppendFile         = 24  //append file
	StorageProtoCmdModifyFile         = 34  //since V3.08
	StorageProtoCmdTruncateFile       = 36  //since V3.08
	StorageProtoCmdGetStorageStatus   = 133

	//for overwrite all old metadata
	StorageSetMetadataFlagOverwriteStr = "O"
	//for replace, insert when the meta item not exist, otherwise update it
	StorageSetMetadataFlagMergeStr = "M"

	FieldSeperator  = "\x02"
	RecordSeperator = "\x01"
)

type MetaData struct {
	Name  string `struc:"[64]byte"`  //FDFS_MAX_META_NAME_LEN
	Value string `struc:"[256]byte"` //FDFS_MAX_META_VALUE_LEN
}

func (m *MetaData) String() string {
	return m.Name + FieldSeperator + m.Value
}

type UploadSlave struct {
	Header
	MasterFileNameLen int    `struc:"int64,sizeof=MasterFileName"`
	FileSize          int    `struc:"int64"`
	PrefixName        string `struc:"[16]byte"` //FilePrefixMaxLen
	FileExtName       string `struc:"[6]byte"`  //FileExtNameMaxLen
	MasterFileName    string
}

type UploadFile struct {
	Header
	StorePathIndex int    `struc:"byte"`
	AppID          int    `struc:"int32"`
	TimeID         int    `struc:"int32"`
	FileSize       int    `struc:"int64"`
	FileExtName    string `struc:"[6]byte"` //FileExtNameMaxLen
}

type FileResult struct {
	GroupName      string `struc:"[16]byte"` //GroupNameMaxLen
	RemoteFileName string
}

type ModifyFile struct {
	Header
	FileNameLen int `struc:"int64,sizeof=FileName"`
	Offset      int `struc:"int64"`
	Length      int `struc:"int64"`
	FileName    string
}

type TruncateFile struct {
	Header
	FileNameLen int `struc:"int64,sizeof=FileName"`
	Offset      int `struc:"int64"`
	FileName    string
}

type AppendFile struct {
	Header
	FileNameLen int `struc:"int64,sizeof=FileName"`
	Size        int `struc:"int64"`
	FileName    string
}

type CreateLinkFile struct {
	Header
	MasterFileNameLen int    `struc:"int64,sizeof=MasterFileName"`
	SrcFileNameLen    int    `struc:"int64,sizeof=SrcFileName"`
	SrcFileSigLen     int    `struc:"int64,sizeof=SrcFileSig"`
	GroupName         string `struc:"[16]byte"` //GroupNameMaxLen
	PrefixName        string `struc:"[16]byte"` //FilePrefixMaxLen
	FileExtName       string `struc:"[6]byte"`  //FileExtNameMaxLen
	MasterFileName    string
	SrcFileName       string
	SrcFileSig        string
}

type SetMeta struct {
	Header
	FileNameLen  int    `struc:"int64,sizeof=FileName"`
	PackMetaSize int    `struc:"int64,sizeof=PackMeta"`
	Flag         string `struc:"[1]byte"`
	GroupName    string `struc:"[16]byte"` //GroupNameMaxLen
	FileName     string
	PackMeta     string
}

type AppIdStatus struct {
	AppId string
	DayId []string
}

type StorePathStatus struct {
	Valid       bool
	AppIdStatus []*AppIdStatus
}

type ServerStatus struct {
	Id         string
	IpAddr     string
	Active     bool
	StorePaths []*StorePathStatus
}

type GroupStatus struct {
	GroupName string
	Servers   []*ServerStatus
}

type TimedStorageStatus struct {
	Groups []*GroupStatus
}

func (s *TimedStorageStatus) GetByGroupName(groupName string) *GroupStatus {
	for _, v := range s.Groups {
		if v.GroupName == groupName {
			return v
		}
	}
	return nil
}
