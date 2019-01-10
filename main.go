package main

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hornbill/color"
	"github.com/hornbill/goApiLib"
	"github.com/hornbill/goHornbillHelpers"
	"github.com/hornbill/pb"

	//SQL Drivers
	_ "github.com/hornbill/go-mssqldb" //Microsoft SQL Server driver - v2005+
	_ "github.com/hornbill/mysql"      //MySQL v4.1 to v5.x and MariaDB driver
)

func main() {
	timeNow = time.Now().Format("2006-01-02 15:04:05")
	logFile = "dataexport_" + time.Now().Format("20060102150405") + ".log"
	flag.StringVar(&configFileName, "file", "conf.json", "Name of the configuration file to load")
	flag.BoolVar(&configDebug, "debug", false, "Debug mode - additional logging")
	flag.Parse()

	hornbillHelpers.Logger(3, "---- "+toolName+" v"+version+" ----", true, logFile)
	hornbillHelpers.Logger(3, "Flag - Configuration File: "+configFileName, true, logFile)
	hornbillHelpers.Logger(3, "Flag - Debug: "+fmt.Sprintf("%v", configDebug), true, logFile)

	//-- Load Configuration File Into Struct
	apiCallConfig, boolConfLoaded = loadConfig()
	if !boolConfLoaded {
		hornbillHelpers.Logger(4, "Unable to load config, process closing.", true, logFile)
		return
	}
	hornbillHelpers.Logger(3, "Instance ID: "+apiCallConfig.InstanceID, true, logFile)

	//Global XMLMC session
	espXmlmc = apiLib.NewXmlmcInstance(apiCallConfig.InstanceID)
	espXmlmc.SetAPIKey(apiCallConfig.APIKey)
	davEndpoint = apiLib.GetEndPointFromName(apiCallConfig.InstanceID) + "/dav/"

	connString = buildConnectionString()
	if connString == "" {
		hornbillHelpers.Logger(4, "Database Connection String Empty. Check the SQLConf section of your configuration.", true, logFile)
		return
	}

	if configDebug {
		hornbillHelpers.Logger(1, "Database Server: "+apiCallConfig.Database.Server, false, logFile)
		hornbillHelpers.Logger(1, "Database Port: "+strconv.Itoa(apiCallConfig.Database.Port), false, logFile)
		hornbillHelpers.Logger(1, "Database Driver: "+apiCallConfig.Database.Driver, false, logFile)
		hornbillHelpers.Logger(1, "Database Encryption: "+fmt.Sprintf("%v", apiCallConfig.Database.Encrypt), false, logFile)
		hornbillHelpers.Logger(1, "Database Server Authentication: "+apiCallConfig.Database.Authentication, false, logFile)
		hornbillHelpers.Logger(1, "Database: "+apiCallConfig.Database.Database, false, logFile)
		hornbillHelpers.Logger(1, "Database Connection String: "+connString, false, logFile)
	}
	//Run and get report content
	for _, definition := range apiCallConfig.Reports {
		runReport(definition, espXmlmc)
	}

}

func runReport(report reportStruct, espXmlmc *apiLib.XmlmcInstStruct) {
	hornbillHelpers.Logger(3, " ", true, logFile)
	hornbillHelpers.Logger(7, "Running Report: "+report.ReportName+" ["+strconv.Itoa(report.ReportID)+"]", true, logFile)

	espXmlmc.SetParam("reportId", strconv.Itoa(report.ReportID))
	espXmlmc.SetParam("comment", "Run from the goHornbillReport tool")

	XMLMC, xmlmcErr := espXmlmc.Invoke("reporting", "reportRun")
	if xmlmcErr != nil {
		hornbillHelpers.Logger(4, xmlmcErr.Error(), true, logFile)
		return
	}

	var xmlRespon xmlmcReportResponse

	err := xml.Unmarshal([]byte(XMLMC), &xmlRespon)
	if err != nil {
		hornbillHelpers.Logger(4, fmt.Sprintf("%v", err), true, logFile)
		return
	}
	if xmlRespon.MethodResult != "ok" {
		hornbillHelpers.Logger(4, xmlRespon.State.ErrorRet, true, logFile)
		return
	}
	if xmlRespon.RunID > 0 {
		reportComplete := false
		for !reportComplete {
			reportSuccess, reportComplete, reportDetails := checkReport(xmlRespon.RunID, espXmlmc)

			if reportComplete {
				if !reportSuccess {
					return
				}
				getReportContent(reportDetails, espXmlmc, report)
				if report.DeleteReportInstance {
					deleteReportInstance(reportDetails.ReportRun.RunID)
				}
				return
			}
			time.Sleep(time.Second * 3)
		}
	} else {
		hornbillHelpers.Logger(4, "No RunID returned from ", true, logFile)
		return
	}
}

func checkReport(runID int, espXmlmc *apiLib.XmlmcInstStruct) (bool, bool, paramsReportStruct) {

	hornbillHelpers.Logger(3, "Checking Report Run ID ["+strconv.Itoa(runID)+"] for completion...", true, logFile)
	espXmlmc.SetParam("runId", strconv.Itoa(runID))
	XMLMC, xmlmcErr := espXmlmc.Invoke("reporting", "reportRunGetStatus")

	if xmlmcErr != nil {
		hornbillHelpers.Logger(4, xmlmcErr.Error(), true, logFile)
		return false, true, paramsReportStruct{}
	}

	var xmlRespon xmlmcReportStatusResponse

	err := xml.Unmarshal([]byte(XMLMC), &xmlRespon)
	if err != nil {
		hornbillHelpers.Logger(4, fmt.Sprintf("%v", err), true, logFile)
		return false, true, paramsReportStruct{}
	}
	if xmlRespon.MethodResult != "ok" {
		hornbillHelpers.Logger(4, xmlRespon.State.ErrorRet, true, logFile)
		return false, true, paramsReportStruct{}
	}

	switch xmlRespon.Params.ReportRun.Status {
	case "pending":
		fallthrough
	case "started":
		fallthrough
	case "running":
		return false, false, paramsReportStruct{}
	case "completed":
		return true, true, xmlRespon.Params
	case "failed":
		fallthrough
	case "aborted":
		return false, true, paramsReportStruct{}
	}
	return false, false, paramsReportStruct{}
}

func getReportContent(reportOutput paramsReportStruct, espXmlmc *apiLib.XmlmcInstStruct, report reportStruct) {
	for _, v := range reportOutput.Files {
		if v.Type == "csv" {
			var counters counterStruct
			reportFile := getFile(reportOutput.ReportRun, v, espXmlmc, report)
			if reportFile != "" {
				success, csvMap := getRecordsFromCSV(reportFile)
				if success {
					totalRecords := len(csvMap)
					if totalRecords == 0 {
						hornbillHelpers.Logger(3, "No records found within "+v.Name+"...", true, logFile)
					} else {
						hornbillHelpers.Logger(3, "Processing "+strconv.Itoa(totalRecords)+" Records from "+v.Name+"...", true, logFile)
						bar := pb.StartNew(totalRecords)
						for _, reportRow := range csvMap {
							upsertRecord(reportRow, report, &counters)
							bar.Increment()
						}
						bar.Finish()
						hornbillHelpers.Logger(3, "Processing Complete", true, logFile)
						hornbillHelpers.Logger(3, "====Report Processing Statistics====", true, logFile)
						hornbillHelpers.Logger(3, " * "+report.ReportName+" ["+strconv.Itoa(report.ReportID)+"]", true, logFile)
						hornbillHelpers.Logger(3, " * Total Records Found: "+strconv.Itoa(totalRecords), true, logFile)
						hornbillHelpers.Logger(3, " * Rows Affected: "+strconv.Itoa(counters.rowsaffected), true, logFile)
						hornbillHelpers.Logger(3, " * Successful Queries: "+strconv.Itoa(counters.success), true, logFile)
						hornbillHelpers.Logger(3, " * Failed Queries: "+strconv.Itoa(counters.failed), true, logFile)
					}
				}
				if report.DeleteReportLocalFile {
					deleteFile(reportFile)
				}

			}
		}
	}
}

func getFile(reportRun reportRunStruct, file reportFileStruct, espXmlmc *apiLib.XmlmcInstStruct, report reportStruct) string {
	hornbillHelpers.Logger(3, "Retrieving "+strings.ToUpper(file.Type)+" Report File "+file.Name+"...", true, logFile)

	cwd, _ := os.Getwd()
	reportsFolder := cwd + "/reports/"
	//-- If reports folder doesn't dxist then create it
	if _, err := os.Stat(reportsFolder); os.IsNotExist(err) {
		err := os.Mkdir(reportsFolder, 0777)
		if err != nil {
			color.Red("Error Creating Reports Folder %q: %s \r", reportsFolder, err)
			os.Exit(2)
		}
	}

	//Create file for data dump
	reportPath := reportsFolder + file.Name
	out, err := os.Create(reportPath)
	if err != nil {
		hornbillHelpers.Logger(4, "CSV File Creation Failed: "+fmt.Sprintf("%v", err), true, logFile)
		return ""
	}
	defer out.Close()
	reportURL := davEndpoint + "reports/" + strconv.Itoa(reportRun.ReportID) + "/" + reportRun.CSVLink

	req, _ := http.NewRequest("GET", reportURL, nil)
	req.Header.Set("Content-Type", "text/xmlmc")
	req.Header.Set("Authorization", "ESP-APIKEY "+apiCallConfig.APIKey)

	if err != nil {
		hornbillHelpers.Logger(4, fmt.Sprintf("%v", err), true, logFile)
		return ""
	}
	duration := time.Second * time.Duration(30)
	client := &http.Client{Timeout: duration}

	resp, err := client.Do(req)
	if err != nil {
		hornbillHelpers.Logger(4, fmt.Sprintf("%v", err), true, logFile)
		return ""
	}
	defer resp.Body.Close()

	//-- Check for HTTP Response
	if resp.StatusCode != 200 {
		hornbillHelpers.Logger(4, fmt.Sprintf("Invalid HTTP Response: %d", resp.StatusCode), true, logFile)
		io.Copy(ioutil.Discard, resp.Body)
		return ""
	}
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		hornbillHelpers.Logger(4, fmt.Sprintf("%v", err), true, logFile)
		return ""
	}
	hornbillHelpers.Logger(3, "Retrieved report data from "+reportPath, false, logFile)
	return reportPath
}

func deleteFile(filePath string) {
	hornbillHelpers.Logger(3, "Deleting Report Local File...", true, logFile)
	err := os.Remove(filePath)
	if err != nil {
		hornbillHelpers.Logger(4, "Error deleting file:"+fmt.Sprintf("%v", err), false, logFile)
		return
	}
}
func deleteReportInstance(runID int) {
	hornbillHelpers.Logger(3, "Deleting Report Run Instance...", true, logFile)
	espXmlmc.SetParam("runId", strconv.Itoa(runID))
	XMLMC, xmlmcErr := espXmlmc.Invoke("reporting", "reportRunDelete")

	if xmlmcErr != nil {
		hornbillHelpers.Logger(4, xmlmcErr.Error(), true, logFile)
		return
	}

	var xmlRespon xmlmcReportStatusResponse

	err := xml.Unmarshal([]byte(XMLMC), &xmlRespon)
	if err != nil {
		hornbillHelpers.Logger(4, fmt.Sprintf("%v", err), true, logFile)
		return
	}
	if xmlRespon.MethodResult != "ok" {
		hornbillHelpers.Logger(4, xmlRespon.State.ErrorRet, true, logFile)
		return
	}
}

//loadConfig -- Function to Load Configruation File
func loadConfig() (apiCallStruct, bool) {
	boolLoadConf := true
	//-- Check Config File File Exists
	cwd, _ := os.Getwd()
	configurationFilePath := cwd + "/" + configFileName
	hornbillHelpers.Logger(1, "Loading Config File: "+configurationFilePath, false, logFile)
	if _, fileCheckErr := os.Stat(configurationFilePath); os.IsNotExist(fileCheckErr) {
		hornbillHelpers.Logger(4, "No Configuration File", true, logFile)
		os.Exit(102)
	}
	//-- Load Config File
	file, fileError := os.Open(configurationFilePath)
	//-- Check For Error Reading File
	if fileError != nil {
		hornbillHelpers.Logger(4, "Error Opening Configuration File: "+fmt.Sprintf("%v", fileError), true, logFile)
		boolLoadConf = false
	}

	//-- New Decoder
	decoder := json.NewDecoder(file)
	//-- New Var based on apiCallStruct
	edbConf := apiCallStruct{}
	//-- Decode JSON
	err := decoder.Decode(&edbConf)
	//-- Error Checking
	if err != nil {
		hornbillHelpers.Logger(4, "Error Decoding Configuration File: "+fmt.Sprintf("%v", err), true, logFile)
		boolLoadConf = false
	}
	//-- Return New Config
	return edbConf, boolLoadConf
}
