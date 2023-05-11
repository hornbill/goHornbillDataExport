package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	hornbillHelpers "github.com/hornbill/goHornbillHelpers"
)

// buildConnectionString -- Build the connection string for the SQL driver
func buildConnectionString() string {
	connectString := ""
	if apiCallConfig.Database.Database == "" ||
		apiCallConfig.Database.Authentication == "SQL" && (apiCallConfig.Database.UserName == "" || apiCallConfig.Database.Password == "") {
		//Conf not set - log error and return empty string
		hornbillHelpers.Logger(4, "Database configuration not set.", true, logFile)
		return ""
	}
	hornbillHelpers.Logger(1, "Connecting to Database Server: "+apiCallConfig.Database.Server, true, logFile)

	switch apiCallConfig.Database.Driver {
	case "mssql":
		connectString = "server=" + apiCallConfig.Database.Server
		connectString = connectString + ";database=" + apiCallConfig.Database.Database
		if apiCallConfig.Database.Authentication == "Windows" {
			connectString = connectString + ";Trusted_Connection=True"
		} else {
			connectString = connectString + ";user id=" + apiCallConfig.Database.UserName
			connectString = connectString + ";password=" + apiCallConfig.Database.Password
		}

		if !apiCallConfig.Database.Encrypt {
			connectString = connectString + ";encrypt=disable"
		}
		if apiCallConfig.Database.Port != 0 {
			dbPortSetting := strconv.Itoa(apiCallConfig.Database.Port)
			connectString = connectString + ";port=" + dbPortSetting
		}
	case "mysql":
		connectString = apiCallConfig.Database.UserName
		if apiCallConfig.Database.Password != "" {
			connectString += ":" + apiCallConfig.Database.Password
		}
		connectString = connectString + "@tcp(" + apiCallConfig.Database.Server + ":"
		if apiCallConfig.Database.Port != 0 {
			dbPortSetting := strconv.Itoa(apiCallConfig.Database.Port)
			connectString = connectString + dbPortSetting
		} else {
			connectString = connectString + "3306"
		}
		connectString = connectString + ")/" + apiCallConfig.Database.Database
	}
	return connectString
}

func buildMySQLQuery(reportRecord map[string]string, report reportStruct) (string, map[string]interface{}) {
	//
	strQuery := ""
	strColumns := ""
	strValues := ""
	strOnDupe := ""
	namedData := make(map[string]interface{})

	for repCol, dbCol := range report.Table.Mapping {
		if reportRecord[repCol] != "" {
			if strColumns != "" {
				strColumns += ", "
			}
			if strValues != "" {
				strValues += ", "
			}
			if strOnDupe != "" {
				strOnDupe += ", "
			}

			strColumns += dbCol
			//remove spaces and `` from column name so NamedExec can map values
			strProcessedColumn := processColumnName(dbCol)
			strValues += ":" + strProcessedColumn
			namedData[strProcessedColumn] = reportRecord[repCol]
			strOnDupe += dbCol + " = :" + strProcessedColumn
		}

	}
	strQuery = "INSERT INTO " + report.Table.TableName + " (" + strColumns + ")" + " VALUES (" + strValues + ") "
	strQuery += "ON DUPLICATE KEY UPDATE " + strOnDupe
	return strQuery, namedData
}

func buildMSSQLInsert(reportRecord map[string]string, report reportStruct) (string, map[string]interface{}) {
	//
	strQuery := ""
	strColumns := ""
	strValues := ""
	namedData := make(map[string]interface{})

	for repCol, dbCol := range report.Table.Mapping {
		if reportRecord[repCol] != "" {
			if strColumns != "" {
				strColumns += ", "
			}
			if strValues != "" {
				strValues += ", "
			}

			strColumns += dbCol
			//remove spaces and [] from column name so NamedExec can map values
			strProcessedColumn := processColumnName(dbCol)
			strValues += ":" + strProcessedColumn
			namedData[strProcessedColumn] = reportRecord[repCol]
		}

	}
	strQuery = "INSERT INTO " + report.Table.TableName + " (" + strColumns + ")" + " VALUES (" + strValues + ") "
	return strQuery, namedData
}

func buildMSSQLUpdate(reportRecord map[string]string, report reportStruct) (string, map[string]interface{}) {
	//
	strQuery := ""
	strOnDupe := ""
	namedData := make(map[string]interface{})

	for repCol, dbCol := range report.Table.Mapping {
		if reportRecord[repCol] != "" {
			if strOnDupe != "" {
				strOnDupe += ", "
			}
			//remove spaces and [] from column name so NamedExec can map values
			strProcessedColumn := processColumnName(dbCol)
			namedData[strProcessedColumn] = reportRecord[repCol]
			strOnDupe += dbCol + " = :" + strProcessedColumn
		}

	}
	strProcessedKey := processColumnName(report.Table.PrimaryKey)
	strQuery = "UPDATE " + report.Table.TableName + " SET " + strOnDupe + " WHERE " + report.Table.PrimaryKey + " = :" + strProcessedKey
	return strQuery, namedData
}

func processColumnName(columnName string) string {
	strTrimmer := strings.TrimLeft(columnName, "[")
	strTrimmer = strings.TrimRight(strTrimmer, "]")
	strTrimmer = strings.TrimLeft(strTrimmer, "`")
	strTrimmer = strings.TrimRight(strTrimmer, "`")
	arrTrimmer := strings.Split(strTrimmer, " ")
	return strings.Join(arrTrimmer[:], "")
}

func doesRecordExist(reportRecord map[string]string, report reportStruct) bool {
	pkVal := ""
	//Get primary key column value from report data
	for repCol, dbCol := range report.Table.Mapping {
		if dbCol == report.Table.PrimaryKey {
			pkVal = reportRecord[repCol]
		}
	}
	if pkVal == "" {
		return false
	}
	sqlQuery := "SELECT " + report.Table.PrimaryKey + " FROM " + report.Table.TableName + " WHERE " + report.Table.PrimaryKey + " = ?"

	stmt, _ := db.Preparex(sqlQuery)
	var id string
	err := stmt.Get(&id, pkVal)
	return (err == nil)
}

// upsertRecord -- Query Asset Database for assets of current type
// -- Builds map of assets, returns true if successful
func upsertRecord(reportRecord map[string]string, report reportStruct, counters *counterStruct) {
	namedData := make(map[string]interface{})
	sqlQuery := ""
	//Build Query
	if apiCallConfig.Database.Driver == "mssql" {
		//does record exist?
		recordExists := doesRecordExist(reportRecord, report)
		if recordExists {
			sqlQuery, namedData = buildMSSQLUpdate(reportRecord, report)
		} else {
			sqlQuery, namedData = buildMSSQLInsert(reportRecord, report)
		}
	} else {
		//No need to check if record exists in MySQL, just do an INSERT...ON DUPLICATE KEY
		sqlQuery, namedData = buildMySQLQuery(reportRecord, report)
	}

	if len(namedData) == 0 {
		counters.failed++
		hornbillHelpers.Logger(4, "Unable to map any values from the returned record:", false, logFile)
		jsonRecord, _ := json.Marshal(reportRecord)
		hornbillHelpers.Logger(3, "[RECORD] "+string(jsonRecord), false, logFile)
		jsonMapping, _ := json.Marshal(report.Table.Mapping)
		hornbillHelpers.Logger(3, "[MAPPINGS] "+string(jsonMapping), false, logFile)

		return
	}

	if configDebug {
		//Add query & params to log
		hornbillHelpers.Logger(3, "[DATABASE] Query:"+sqlQuery, false, logFile)
		hornbillHelpers.Logger(3, "[DATABASE] Binding:", false, logFile)
		for k, v := range namedData {
			hornbillHelpers.Logger(3, "[DATABASE] :"+k+" = "+fmt.Sprintf("%v", v), false, logFile)
		}
	}

	results, err := db.NamedExec(sqlQuery, namedData)
	if err != nil {
		hornbillHelpers.Logger(4, " [DATABASE] NamedExec Error: "+fmt.Sprintf("%v", err), true, logFile)
		counters.failed++
		return
	}
	if configDebug {
		hornbillHelpers.Logger(3, "[DATABASE] NamedExec Success", false, logFile)
	}
	counters.success++

	affectedCount, err := results.RowsAffected()
	if err != nil {
		hornbillHelpers.Logger(4, " [DATABASE] RowsAffected Error: "+fmt.Sprintf("%v", err), false, logFile)
		return
	}
	if configDebug {
		hornbillHelpers.Logger(3, "[DATABASE] RowsAffected: "+strconv.FormatInt(affectedCount, 10), false, logFile)
	}

	counters.rowsaffected += int(affectedCount)
}
