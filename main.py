import json
import boto3
import datetime
from datetime import date

sqs_send = boto3.client('sqs')


# Send one message & try to read the same , observer the events in SQS Console

# Defining Custom Function to Send Message to Main Queue and Deleting it from DLQ
def sendQueue(sendQueueUrl, messageBody, messageAttributes):
    # Calling Send Message API
    ret = sqs_send.send_message(QueueUrl=sendQueueUrl,
                                MessageBody=messageBody,
                                MessageAttributes=messageAttributes)
    return ret

sendQueueUrl='https://sqs.eu-west-1.amazonaws.com/606438480380/RanmalNewQueue'


MessageAttributes={
    'Name': {
        'StringValue': 'Ranmal Mendis',
        'DataType': 'String'
    },
    'Age': {
        'StringValue': '5',
        'DataType': 'Number'
    }}
messageBody='Hello World 10'
response = sendQueue(sendQueueUrl,messageBody,MessageAttributes)

messages = sqs_send.receive_message(QueueUrl=sendQueueUrl, MaxNumberOfMessages=10
                                    ,WaitTimeSeconds=20,MessageAttributeNames=['All'])

print(messages)

receipthandle=messages['Messages'][0]['ReceiptHandle']
print(receipthandle)

# Delete the messages
sqs_send.delete_message(QueueUrl=sendQueueUrl, ReceiptHandle=receipthandle)

messages = sqs_send.receive_message(QueueUrl=sendQueueUrl, MaxNumberOfMessages=10
                                    ,WaitTimeSeconds=20,MessageAttributeNames=['All'])
print(messages)

# Note : the "Messages" key is not there when queue is empty

def process_sqs_message():
  while True:
    messages = sqs_send.receive_message(QueueUrl=sendQueueUrl, MaxNumberOfMessages=10
                                      ,WaitTimeSeconds=20,MessageAttributeNames=['All'])
    if 'Messages' in messages:
      for m in messages['Messages']:
        print(m['Body'],m['ReceiptHandle'],m['MessageAttributes'])
        sqs_send.delete_message(QueueUrl=sendQueueUrl, ReceiptHandle=m['ReceiptHandle'])
    else:
      print('Queue is currently Empty or Messages are Invisible')
      break

def process_sqs_message():
  while True:
    messages = sqs_send.receive_message(QueueUrl=sendQueueUrl, MaxNumberOfMessages=10
                                      ,WaitTimeSeconds=20,MessageAttributeNames=['All'])
    if 'Messages' in messages:
      for m in messages['Messages']:
        print(m['Body'],m['ReceiptHandle'])
        sqs_send.delete_message(QueueUrl=sendQueueUrl, ReceiptHandle=m['ReceiptHandle'])
    else:
      print('Queue is currently Empty or Messages are Invisible')
      break

MessageAttributes={
    'Name': {
        'StringValue': 'Ranmal Mendis',
        'DataType': 'String'
    },
    'Age': {
        'StringValue': '5',
        'DataType': 'Number'
    }}
messageBody='Hello World 1'
response = sendQueue(sendQueueUrl,messageBody,MessageAttributes)
MessageAttributes={
    'Age': {
        'StringValue': '5',
        'DataType': 'Number'
    }}
messageBody='Hello World 2'
response = sendQueue(sendQueueUrl,messageBody,MessageAttributes)
MessageAttributes={
    'Name': {
        'StringValue': 'Satadru',
        'DataType': 'String'
    }}
messageBody='Hello World 3'
response = sendQueue(sendQueueUrl,messageBody,MessageAttributes)

process_sqs_message()
