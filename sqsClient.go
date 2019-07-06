// This file implements the functions to interact with the SQS for sending and receiving messages
package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

/*
SetupQueueSession sets up the seesion with the SQS

*/
func SetupQueueSession(queueName string) (*sqs.SQS, *string) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "test-config"),
	})

	if err != nil {
		fmt.Println("error", err)
	}

	svc := sqs.New(sess)
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			fmt.Println("Unable to find queue", queueName)
		}
		fmt.Println("Unable to queue ", queueName, err)

	}
	return svc, resultURL.QueueUrl

}

/*
ReceiveMessage received the message from the queue specified by the qURL,
	using the session specified by svc
*/
func ReceiveMessage(svc *sqs.SQS, qURL *string, maxMessages int64) {
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            qURL,
		MaxNumberOfMessages: aws.Int64(maxMessages),
		VisibilityTimeout:   aws.Int64(60), // 60 seconds
		WaitTimeSeconds:     aws.Int64(0),
	})
	if err != nil {
		fmt.Println("Error", err)
		return
	}
	if len(result.Messages) == 0 {
		fmt.Println("Received no messages")
		return
	}
	fmt.Printf("Success: %+v\n", result.Messages)
}

/*
SendMessage sends the message to the queue specified by the qURL
	using the session specified by svc
*/
func SendMessage(message string, svc *sqs.SQS, qURL *string) {
	result, err := svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),
		MessageBody:  aws.String(message),
		QueueUrl:     qURL,
	})

	if err != nil {
		fmt.Println("Error", err)
		return
	}

	fmt.Println("Success", *result.MessageId)
}
