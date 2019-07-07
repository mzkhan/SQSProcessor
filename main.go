package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

type Message struct {
	Value string `json:"message"`
}

func main() {
	address := "127.0.0.1:8080"
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
	svc := SetupQueueSession()
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
	svc := SetupQueueSession()
	queueURL := GetQueueURL(svc, queueName)

	result, err := ReceiveMessage(svc, queueURL, 10)
	if err != nil {
		http.Error(rs, err.Error(), 500)
		return
	}
	log.Println("Messages Received: ", len(result.Messages))

	for _, msg := range result.Messages {
		_, errDelete := DeleteMessage(msg, svc, queueURL)
		if errDelete != nil {
			http.Error(rs, err.Error(), 500)
			log.Println("Error in deleting", errDelete)
		} else {
			log.Println("Deleted Message successfully: ", msg.Body)
		}
	}
	output, err := json.Marshal(result)
	if err != nil {
		http.Error(rs, err.Error(), 500)
		return
	}
	rs.WriteHeader(http.StatusOK)
	rs.Header().Set("content-type", "application/json")
	rs.Write(output)
}
