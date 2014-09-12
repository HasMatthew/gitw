package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-log/log"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/dynamodb"
	"github.com/goamz/goamz/s3"
	"github.com/goamz/goamz/sqs"
)

const s3Bucket = "mat_scratch"
const accessKey = "AKIAJN5MMZYW7MAU27NQ"
const secretKey = "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"
const logTimeLayout = "2006-01-02T15:04:05Z"
const filenameTimeLayout = "20060102T1504Z"

var dynamoTable string
var locations chan *LocationResponse = make(chan *LocationResponse, 1000000)
var infoLog bool
var queueName string
var logSeparator = []byte(" ")
var awsRegion aws.Region = aws.USEast
var awsAuth aws.Auth = aws.Auth{
	AccessKey: accessKey,
	SecretKey: secretKey,
}
var pk dynamodb.PrimaryKey = dynamodb.PrimaryKey{
	KeyAttribute: &dynamodb.Attribute{
		Type: "S",
		Name: "hash",
	},
}

func main() {
	AutoGOMAXPROCS()

	port := os.Getenv("GITW_PORT")
	hostNum := os.Getenv("HOST_NUM")
	if hostNum == "" {
		log.Fatalln("HOST_NUM must be set!")
	}
	infoLog = os.Getenv("INFO_LOG") != "off"
	workers, err := strconv.Atoi(os.Getenv("WORKERS"))
	if err != nil {
		log.Fatalln("Error parsing WORKERS: ", err)
	}
	dynamoTable = os.Getenv("DYNAMO_TABLE")
	if dynamoTable == "" {
		log.Fatalln("DYNAMO_TABLE must be set!")
	}

	queueName = "gitw_" + hostNum

	// go processLog(locations)
	if infoLog {
		log.Infof("Starting %d workers...", workers)
	}
	var outs []chan *LocationResponse
	for i := 0; i < workers; i++ {
		ch := make(chan *LocationResponse, 128)
		outs = append(outs, ch)
		go processLogWorker(ch)
	}

	go distribute(locations, outs)

	s := &http.Server{
		Addr:           ":" + port,
		Handler:        &Handler{},
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Infof("Listening on port %s...\n", port)
	log.Fatalln(s.ListenAndServe())
}

func AutoGOMAXPROCS() {
	if os.Getenv("GOMAXPROCS") == "" {
		cpus := runtime.NumCPU()
		log.Infof("GOMAXPROCS environment variable not set. Defaulting to system CPU count: %d", cpus)

		runtime.GOMAXPROCS(cpus)
	} else {
		log.Infof("GOMAXPROCS environment variable set, continuing")
	}
}

type GUIDRequest struct {
	Guid string `json:"guid"`
}

type GUIDResponse struct {
	GuidHash   string `json:"guid_hash"`
	BucketName string `json:"bucket_name"`
}

type SQSMessage struct {
	GuidHash   string `json:"guid_hash"`
	ResultHash string `json:"result_hash"`
}

// consider using this instead of a map for dynamo response
type LocationResponse struct {
	Hash     string `json:"hash"`
	Bucket   string `json:"bucket"`
	RandLine string `json:"randLine"`
}

// Handler for serving requests
type Handler struct {
}

func (mux *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	guidRequest := &GUIDRequest{}
	d := json.NewDecoder(request.Body)
	if err := d.Decode(&guidRequest); err != nil {
		log.Error("Error decoding JSON: ", err)
		http.Error(writer, err.Error(), 500)
		return
	}

	dynamo := dynamodb.Server{
		Auth:   awsAuth,
		Region: aws.USEast,
	}

	locationsTable := dynamo.NewTable(dynamoTable, pk)

	// lls, err := locationsTable.Scan([]dynamodb.AttributeComparison{
	// 	*dynamodb.NewStringAttributeComparison("hash", dynamodb.COMPARISON_NOT_EQUAL, "asdf"),
	// })
	// for _, ll := range lls {
	// 	log.Info(ll["hash"].Value)
	// }

	digest := sha256.Sum256([]byte(guidRequest.Guid))
	hashKey := hex.EncodeToString(digest[:])
	item, err := locationsTable.GetItem(&dynamodb.Key{HashKey: hashKey})
	// item, err := locationsTable.GetItem(&dynamodb.Key{HashKey: guidRequest.Guid})
	if err != nil {
		log.Error("Error getting item from table: ", err)
		http.Error(writer, err.Error(), 500)
		return
	}

	location := &LocationResponse{
		Hash:     hashKey,
		Bucket:   item["bucket"].Value,
		RandLine: item["randLine"].Value,
	}

	if infoLog {
		log.Infof("S3 key: %s", location.Bucket)
	}

	response := GUIDResponse{
		GuidHash:   location.Hash,
		BucketName: location.Bucket,
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Error("Error marshalling JSON: ", err)
		http.Error(writer, err.Error(), 500)
		return
	}

	locations <- location
	// go processLog(location)

	writer.Write(jsonResponse)
}

func processLog(location *LocationResponse) {
	// log.Info("Line number: ", location.RandLine)
	lineNumber, err := strconv.Atoi(location.RandLine)
	if err != nil {
		log.Error("Error converting randLine to int: ", err)
		return
	}

	reader, err := s3.New(awsAuth, awsRegion).Bucket(s3Bucket).GetReader(location.Bucket)
	if err != nil {
		log.Error("Error getting S3 reader: ", err)
		return
	}
	bufReader := bufio.NewReader(reader)
	defer reader.Close()

	s3KeyParts := strings.SplitN(location.Bucket, "_", 7)
	filenameTimestamp, err := time.Parse(filenameTimeLayout, s3KeyParts[5])
	if err != nil {
		log.Error("Error parsing filename timestamp: ", err)
		return
	}
	if infoLog {
		log.Info("Filename timestamp: ", filenameTimestamp.String())
	}

	var sumT int = 0
	sha := sha256.New()
	for i := 0; i < lineNumber; i++ {
		line, _, err := bufReader.ReadLine()
		if err != nil {
			log.Error("Error reading from S3 buffered: ", err)
			return
		}

		splitLine := bytes.SplitN(line, logSeparator, 14)

		if len(splitLine) < 13 {
			return
		}
		sha.Write(splitLine[12])

		logTimestamp, err := time.Parse(logTimeLayout, string(splitLine[0]))
		if err != nil {
			log.Error("Error parsing log line timestamp: ", err)
			return
		}
		if infoLog {
			log.Info("Log timestamp: ", logTimestamp.String())
		}

		timeDifference := int(filenameTimestamp.Sub(logTimestamp).Seconds())
		if timeDifference < 0 {
			timeDifference = 0
		}

		if infoLog {
			log.Infof("Time diff: %d", timeDifference)
		}
		sumT += timeDifference
	}

	if infoLog {
		log.Infof("Time difference sum: %d", sumT)
	}

	cksum := sha.Sum(nil)

	if infoLog {
		log.Info("cksum1: ", hex.EncodeToString(cksum))
	}

	for i := 0; i < sumT+1; i++ {
		sha := sha256.New()
		sha.Write(cksum)
		cksum = sha.Sum(nil)
	}

	message, err := json.Marshal(SQSMessage{
		GuidHash:   location.Hash,
		ResultHash: hex.EncodeToString(cksum),
	})
	if err != nil {
		log.Error("Error marshalling SQS message JSON: ", err)
		return
	}
	if infoLog {
		log.Info("SQS message: ", string(message))
	}

	queue, err := sqs.New(awsAuth, awsRegion).GetQueue(queueName)
	if err != nil {
		log.Error("Error getting queue: ", err)
		return
	}
	queue.SendMessage(string(message))
}

func distribute(in <-chan *LocationResponse, outs []chan *LocationResponse) {
	chans := len(outs)
	for i := 0; ; i++ {
		location := <-in
		if infoLog {
			log.Infof("Distributing location %s to chan %d", location.Bucket, i%chans)
		}
		outs[i%chans] <- location
	}
}

func processLogWorker(in <-chan *LocationResponse) {
	if infoLog {
		log.Info("Starting worker...")
	}
	for {
		location := <-in
		if infoLog {
			log.Info("Processing: ", location.Bucket)
		}
		processLog(location)
	}
}
