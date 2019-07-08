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
func SetupQueueSession(configName string, region string) *sqs.SQS {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewSharedCredentials("", configName),
	})

	if err != nil {
		fmt.Println("error", err)
	}

	svc := sqs.New(sess)

	return svc //, resultURL.QueueUrl

}

/*
GetQueueURL received the message from the queue specified by the qURL,
	using the session specified by svc
*/
func GetQueueURL(svc *sqs.SQS, queueName string) *string {
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			fmt.Println("Unable to find queue", queueName)
		}
		fmt.Println("Unable to queue ", queueName, err)

	}
	return resultURL.QueueUrl
}

/*
ReceiveMessage received the message from the queue specified by the qURL,
	using the session specified by svc
*/
func ReceiveMessage(svc *sqs.SQS, qURL *string, maxMessages int64) (*sqs.ReceiveMessageOutput, error) {
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            qURL,
		MaxNumberOfMessages: aws.Int64(maxMessages),
		VisibilityTimeout:   aws.Int64(60), // 60 seconds
		WaitTimeSeconds:     aws.Int64(0),
	})

	return result, err
}

/*
SendMessage sends the message to the queue specified by the qURL
	using the session specified by svc
*/
func SendMessage(message string, svc *sqs.SQS, qURL *string) (*sqs.SendMessageOutput, error) {
	result, err := svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),
		MessageBody:  aws.String(message),
		QueueUrl:     qURL,
	})
	return result, err

}

/*
DeleteMessage sends the message to the queue specified by the qURL
	using the session specified by svc
*/
func DeleteMessage(message *sqs.Message, svc *sqs.SQS, qURL *string) (*sqs.DeleteMessageOutput, error) {
	result, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      qURL,
		ReceiptHandle: message.ReceiptHandle,
	})
	return result, err
}
