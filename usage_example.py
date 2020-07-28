import dqs  # i.e. Daupler DynamoDB Queue Service
import json

# Create a client

client = dqs.client()

# Queue creation
queueName='AWSBlog-shipment-demo'

client.create_queue(
  QueueName = queueName,
  QueueType = 'priority'
)

# Producer

myMessage1 = {'called_phone': '555-123-1234', 'calling_phone': '555-321-4321'}
myMessage2 = {'called_phone': '555-123-1234', 'calling_phone': '555-321-4321', 'someproperty': 'somevalue'}

response = client.send_message(
  QueueName = queueName,
  MessageBody = json.dumps(myMessage1),
  Priority = 100
)

# Alternatively:

queue = dqs.queue('outboundCalls')

response = queue.send_message(
  MessageBody = json.dumps(myMessage1),
  Priority = 100
)

# Response Syntax
# {
#     'MessageId': 'string',  (a uuid)
# }

# This jumps ahead of message with priority of 100 above (if still queued)
client.send_message(
  QueueName = queueName,
  MessageBody = json.dumps(myMessage2),
  Priority = 50 
)


# Consumer 

response = client.receive_message(
  QueueName = queueName
)

# Alternatively:

queue = dqs.queue(queueName)
response = queue.receive_message()

# Response Format:
# 
#     'Messages': [
#         {
#             'MessageId': 'string',
#             'ReceiptHandle': 'string',
#             'Body': 'string',
#         },
#     ]

message = {'ReceiptHandle':  42}

client.delete_message(
  QueueName = 'outboundCalls',
  ReceiptHandle = message['ReceiptHandle']
)

# Alternatively:

queue = dqs.queue(queueName)
queue.delete_message(ReceiptHandle = message['ReceiptHandle'])

