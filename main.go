package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

//export
//copy retries ("ID","RetryID","Action","Payload","RetryCount","Type") to 'dump.csv' with delimiter = '|';
//
//import
//copy retries ("ID","RetryID","Action","Payload","RetryCount","Type","Timestamp","BucketID","ErrorMessage","UpdatedAt") from 'result.csv' with delimiter='|' and maxbatchsize = 1 ;

func main() {
	fmt.Println("Start...")
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
	writerFile, err := os.OpenFile("./result.csv", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer writerFile.Close()

	text, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}

	timestamp := time.Now()

	for _, line := range text {
		t := timestamp.Format("2006-01-02 15:04:05")

		bucketID := int(time.Duration(timestamp.UnixNano()).Minutes() / 15)

		line = append(line, t, strconv.Itoa(bucketID), "unknown", t) // append Timestamp, BucketID, ErrorMessage, UpdatedAt

		str := strings.Join(line, "|")

		_, err = writerFile.WriteString(str + "\r\n")
		if err != nil {
			fmt.Println(err)
			return
		}

		timestamp = timestamp.Add(-1 * time.Second)
	}

	fmt.Println("Done")
}
