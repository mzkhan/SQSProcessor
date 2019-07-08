# SQSProcessor
Go implementation of SQS Message processing

## Pre-requisitives

create a file named 
.aws/credentials with the following data

```
[default]
aws_access_key_id = <access_key_id>
aws_secret_access_key = <access_key_value>

[<config_name>]
aws_access_key_id = <access_key_id>
aws_secret_access_key = <<access_key_value>
```


## Starting the server

`$ go run main.go "test-config" "us-east-1" `

# Send Message
```html
POST /message?queueName=queue1
Content-Type: application/json
Accept: application/json
Body {"message": "Message12"}
```

# Consume Message
```html
GET /message?queueName=queue1
```
