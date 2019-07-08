package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type Message struct {
	Value string `json:"message"`
}

// var wg sync.WaitGroup
var terminatePoll chan int64

// We can extend this code to add a poller for one message queue,
//	by creating a map for a channel and a poller and sending the message
//  to the channel to start and stop the poller for the specific queue

func main() {
	terminatePoll = make(chan int64)
	go StartPoll("queue1")
	address := "127.0.0.1:8080"
	log.Println("Starting server on address", address)
	http.HandleFunc("/message", handleMessage)
	http.HandleFunc("/stopPoll", handleStopPoll)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		panic(err)
	}
}

func handleStopPoll(rs http.ResponseWriter, rq *http.Request) {
	log.Println("Stop Poll Signal received")
	go func() {
		terminatePoll <- 1
	}()
	rs.WriteHeader(http.StatusAccepted)
}

func handleMessage(rs http.ResponseWriter, rq *http.Request) {
	queueName, ok := rq.URL.Query()["queueName"]
	if !ok || len(queueName[0]) < 1 {
		log.Println("Url Param 'queueName' is missing")
		http.Error(rs, "Bad Request", http.StatusBadRequest)
		return
	}
	if rq.Method != "POST" {
		http.Error(rs, "Bad Method Type. Only POST method supported", http.StatusBadRequest)
		return
	}

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
	svc := SetupQueueSession()
	queueURL := GetQueueURL(svc, queueName[0])

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

func StartPoll(queueName string) {
	svc := SetupQueueSession()
	queueURL := GetQueueURL(svc, queueName)
	log.Println("Starting the polling for queue", queueName)
	for {
		select {
		case <-terminatePoll:
			log.Println("Stopping the polling as received stop signal")
			return
		default:
			result, err := ReceiveMessage(svc, queueURL, MAX_MESSAGES_RECEIVE, RECEIVE_WAIT_TIME)
			if err != nil {
				log.Fatal("Unable to receive message", err)
			}
			msgCount := len(result.Messages)

			// In case no message is received, we Sleep for 60 seconds
			if msgCount == 0 {
				log.Println("No Messages in the Queue", queueName, "\nWaiting for ", POLL_BACK_OFF_TIME, "seconds")
				time.Sleep(time.Duration(POLL_BACK_OFF_TIME) * time.Second)
				continue
			}
			// For each of the message, we are spawning a new thread for message consumption
			//	We can wait until all the messages received are processed before
			//	making another receive call, however, in case there is one message that causes the 
			//	thread to stall, we will have no message getting consumed.
			//	So, commenting out the wait group usage, and adding a 30 seconds wait after message
			//	dequeuing.

			log.Println("Messages Received: ", msgCount)

			// wg.Add(msgCount)
			for _, msg := range result.Messages {
				go ConsumeMessage(msg, svc, queueURL)
			}
			// wg.Wait()
			log.Println("Message processing threads dispatched \nWaiting for 30 seconds before the next receive")
				time.Sleep(time.Duration(30) * time.Second)
		}
	}

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

func ConsumeMessage(message *sqs.Message, svc *sqs.SQS, qURL *string) (string, error) {
	// Commenting out the waitGroup usage to avoid system stall because of one message
	// defer wg.Done()
	returnMessage, err := ProcessMessage(message)
	if err != nil {
		// In case of an error, we can retry and send the message to
		//	a DeadLetter queue after multiple failures.
		//	However, AWS provides a functionality where we can move a message
		//	to a DLQ after multiple times its received but not deleted.
		//	https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
		return returnMessage, err
	}
	_, errDelete := DeleteMessage(message, svc, qURL)
	if errDelete != nil {
		log.Println("Error in deleting", errDelete)
		return "Deletion Failed", errDelete
	}
	log.Println("Deleted Message successfully: ", message.Body)
	return "success", nil
}
