package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	args := os.Args[1:]
	fmt.Println("Arguments", args)
	if len(args) < 1 {
		fmt.Println("No Argument provided")
		return
	}

	queueName := "queue1"

	svc, qURL := SetupQueueSession(queueName)
	if args[0] == "sendMessage" {
		if len(args) < 2 {
			fmt.Println("No Message provided")
			return
		}
		message := args[1]
		fmt.Println("Message:", message)
		SendMessage(message, svc, qURL)

	} else if args[0] == "receiveMessage" {
		var messageCount int64 = 1
		if len(args) == 2 {
			messageCount, err := strconv.ParseInt(args[1], 0, 64)
			if err != nil {
				fmt.Println("Error while parsing second Param\n", err, messageCount)
				return
			}
		}
		ReceiveMessage(svc, qURL, messageCount)
	}
}
