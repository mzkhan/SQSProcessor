package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type Message struct {
	Value string `json:"message"`
}

type ChannelMessage struct {
	message *sqs.Message
	qURL    *string
}

//var MessageChannel chan ChannelMessage
var MessageChannel chan *sqs.Message

var svc *sqs.SQS

func main() {
	args := os.Args[1:]
	configName := "default"
	region := "us-east-1"
	if len(args) > 0 {
		configName = args[0]
		region = args[1]
	}

	MessageChannel = make(chan *sqs.Message)
	svc = SetupQueueSession(configName, region)
	address := ":8080"
	log.Println("Starting server on address", address)
	http.HandleFunc("/message", handle_message)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		panic(err)
	}

}

func handle_message(rs http.ResponseWriter, rq *http.Request) {
	queueName, ok := rq.URL.Query()["queueName"]
	if !ok || len(queueName[0]) < 1 {
		log.Println("Url Param 'queueName' is missing")
		http.Error(rs, "Bad Request", 500)
		return
	}

	switch rq.Method {
	case "GET":
		HandleReceiveMessage(rs, rq, queueName[0])
		break
	case "POST":
		HandleSendMessage(rs, rq, queueName[0])
	}

}

func HandleSendMessage(rs http.ResponseWriter, rq *http.Request, queueName string) {
	message, err := ioutil.ReadAll(rq.Body)
	defer rq.Body.Close()
	if err != nil {
		rs.WriteHeader(http.StatusBadRequest)
	}

	// Unmarshal
	var msg Message
	err = json.Unmarshal(message, &msg)
	if err != nil {
		http.Error(rs, err.Error(), 500)
		return
	}

	//setup the SQS client session and get the queue URL

	queueURL := GetQueueURL(svc, queueName)
	result, err := SendMessage(msg.Value, svc, queueURL)
	if err != nil {
		http.Error(rs, err.Error(), 500)
		return
	}

	log.Println("Message send succeeded")
	output, err := json.Marshal(result)
	if err != nil {
		http.Error(rs, err.Error(), 500)
		return
	}
	rs.WriteHeader(http.StatusCreated)
	rs.Header().Set("content-type", "application/json")
	rs.Write(output)
}

func HandleReceiveMessage(rs http.ResponseWriter, rq *http.Request, queueName string) {
	//setup the SQS client session and get the queue URL
	queueURL := GetQueueURL(svc, queueName)
	result, err := ReceiveMessage(svc, queueURL, 10)
	if err != nil {
		http.Error(rs, err.Error(), 500)
		return
	}
	msgCount := len(result.Messages)
	log.Println("Messages Received: ", msgCount)

	// For each of the message, we are spawning a new thread for message consumption
	//	We can have a wait and signal mechanism to wait till all the processing is completed
	//	However, this make the message receive API synchronous

	//wg.Add(msgCount)
	for _, msg := range result.Messages {
		go ConsumeMessage(queueURL)
		go func() {
			MessageChannel <- msg
		}()
	}
	// wg.Wait()

	log.Println("All messages dispatched for consumption")
	output, err := json.Marshal(result)
	if err != nil {
		http.Error(rs, err.Error(), 500)
		return
	}
	rs.WriteHeader(http.StatusOK)
	rs.Header().Set("content-type", "application/json")
	rs.Write(output)
}

func ProcessMessage(message *sqs.Message) (string, error) {
	// This is the function that will add the processing logic
	// for each of the received message

	// The implementation should be such that in case of multiple
	//	delivery of a message, the processing should be idempotent

	//Adding a wait of 10 seconds to account for processing time

	log.Println("In Process Message. MessageID: ", message)
	time.Sleep(time.Duration(10) * time.Second)
	log.Println("Message Processing Completed")
	return "Success", nil
}

func ConsumeMessage(qURL *string) (string, error) {

	// Uncomment if we want to have a join for each of message consumption thread
	// defer wg.Done()
	message := <-MessageChannel
	returnMessage, err := ProcessMessage(message)
	if err != nil {
		return returnMessage, err
	}
	_, errDelete := DeleteMessage(message, svc, qURL)
	if errDelete != nil {
		log.Fatal("Error in deleting", errDelete)
		return "Deletion Failed", errDelete
	}
	log.Println("Deleted Message successfully: ", message.Body)
	return "success", nil
}
