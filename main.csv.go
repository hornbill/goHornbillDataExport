package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/360EntSecGroup-Skylar/excelize"

	hornbillHelpers "github.com/hornbill/goHornbillHelpers"
)

func getRecordsFromCSV(csvFile string) (bool, []map[string]string) {
	rows := []map[string]string{}
	file, err := os.Open(csvFile)
	if err != nil {
		hornbillHelpers.Logger(4, "Error opening CSV file: "+fmt.Sprintf("%v", err), true, logFile)
		return false, rows
	}
	defer file.Close()

	bom := make([]byte, 3)
	file.Read(bom)
	if bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		// BOM Detected, continue with feeding the file
	} else {
		// No BOM Detected, reset the file feed
		file.Seek(0, 0)
	}

	r := csv.NewReader(file)
	r.LazyQuotes = true

	var header []string

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			hornbillHelpers.Logger(4, "Error reading CSV data: "+fmt.Sprintf("%v", err), true, logFile)
			return false, rows
		}
		if header == nil {
			header = record
		} else {
			dict := map[string]string{}
			for i := range header {
				dict[header[i]] = record[i]
			}
			rows = append(rows, dict)
		}
	}
	return true, rows

}

func getRecordsFromXLSX(xlsxFile string) (bool, []map[string]string) {
	rows := []map[string]string{}
	f, err := excelize.OpenFile(xlsxFile)
	if err != nil {
		hornbillHelpers.Logger(4, "Error opening XLSX file: "+err.Error(), true, logFile)
		return false, rows
	}

	// Get all the rows in the Sheet1.
	rowsArr, err := f.GetRows("Sheet1")
	if err != nil {
		hornbillHelpers.Logger(4, "Error reading XLSX file: "+err.Error(), true, logFile)
		return false, rows
	}
	var header []string
	for _, row := range rowsArr {
		if header == nil {
			header = row
		} else {
			dict := map[string]string{}
			for i := range header {
				dict[header[i]] = row[i]
			}
			rows = append(rows, dict)
		}
	}

	return true, rows
}
