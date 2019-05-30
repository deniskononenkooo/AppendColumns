package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
)

//export
//copy retries ("ID","RetryID","Action","Payload","RetryCount","Type") to 'dump.csv' with delimiter = '|';
//
//import
//copy retries ("ID","RetryID","Action","Payload","RetryCount","Type","Timestamp","BucketID","ErrorMessage","UpdatedAt") from 'results.csv' with delimiter='|' and maxbatchsize = 1 ;

func main() {
	//Open csv file with existing columns
	readerFile, err := os.Open("./dump.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer readerFile.Close()

	reader := csv.NewReader(readerFile)
	reader.Comma = '|'
	reader.LazyQuotes = true

	//Create and open csv file with appended columns
	writerFile, err := os.OpenFile("./result.csv", os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer writerFile.Close()

	writer := csv.NewWriter(writerFile)
	writer.Comma = '|'

	text, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}

	timestamp := time.Now()

	for _, line := range text {
		t := timestamp.Format("2006-01-02 15:04:05")

		bucketID := int(time.Duration(timestamp.UnixNano()).Minutes() / 15)

		line = append(line, t, strconv.Itoa(bucketID), "", t) // append Timestamp, BucketID, ErrorMessage, UpdatedAt

		err = writer.Write(line)
		if err != nil {
			fmt.Println(err)
			return
		}

		timestamp = timestamp.Add(-1 * time.Second)
	}

	writer.Flush()
}
