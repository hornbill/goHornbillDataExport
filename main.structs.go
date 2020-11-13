package main

import apiLib "github.com/hornbill/goApiLib"

const (
	version  = "1.6.0"
	toolName = "Hornbill Data Export Tool"
)

var (
	apiCallConfig    apiCallStruct
	boolConfLoaded   bool
	configDebug      bool
	configFileName   string
	configVersion    bool
	configTimeout    int
	configSkipInsert bool
	connString       string
	davEndpoint      string
	espXmlmc         *apiLib.XmlmcInstStruct
	logFile          string
	timeNow          string
)

type counterStruct struct {
	success      int
	failed       int
	rowsaffected int
}

type apiCallStruct struct {
	APIKey     string
	InstanceID string
	Database   struct {
		Driver         string
		Server         string
		Database       string
		Authentication string
		UserName       string
		Password       string
		Port           int
		Encrypt        bool
	}
	Reports []reportStruct
}

type reportStruct struct {
	ReportID              int
	ReportName            string
	DeleteReportInstance  bool
	DeleteReportLocalFile bool
	UseXLSX               bool
	Table                 dbConfigStruct
}

type dbConfigStruct struct {
	TableName  string
	PrimaryKey string
	Mapping    map[string]string
}

type stateStruct struct {
	Code     string `xml:"code"`
	ErrorRet string `xml:"error"`
}

type xmlmcReportResponse struct {
	MethodResult string      `xml:"status,attr"`
	State        stateStruct `xml:"state"`
	RunID        int         `xml:"params>runId"`
}

type xmlmcReportStatusResponse struct {
	MethodResult string             `xml:"status,attr"`
	State        stateStruct        `xml:"state"`
	Params       paramsReportStruct `xml:"params"`
}

type paramsReportStruct struct {
	ReportRun reportRunStruct    `xml:"reportRun"`
	Files     []reportFileStruct `xml:"files"`
}

type reportRunStruct struct {
	RunID    int    `xml:"runId"`
	ReportID int    `xml:"reportId"`
	Status   string `xml:"status"`
	RunBy    string `xml:"runBy"`
	CSVLink  string `xml:"csvLink"`
}

type reportFileStruct struct {
	Name string `xml:"name"`
	Type string `xml:"type"`
}

//WriteCounter - Stores many bytes have been downloaded from the report on the instance
type WriteCounter struct {
	Total    uint64
	FileName string
}
