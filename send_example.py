import dqs 
import json
import logging
import logconfig

log = logging.getLogger(__name__)

# Create a client

client = dqs.client()

# Queue creation
# queueName='AWSBlog-shipment-demo'
queueName = 'MyNewQueue5'

client.create_queue(
  QueueName = queueName,
  QueueType = 'priority'
)

# Producer

myMessage1 = {'called_phone': '555-123-1234', 'calling_phone': '555-321-4320', 'something': 'else'}
myMessage2 = {'called_phone': '555-123-1234', 'calling_phone': '555-321-4321', 'someproperty': 'somevalue'}
myMessage3 = {'called_phone': '555-123-1234', 'calling_phone': '555-321-4322', 'someproperty': 'some other value'}

response = client.send_message(
  QueueName = queueName,
  MessageBody = json.dumps(myMessage1),
  Priority = 100
)

log.info(f"Response from send_message is {response}")

response = client.send_message(
  QueueName = queueName,
  MessageBody = json.dumps(myMessage2),
  Priority = 10
)

log.info(f"Response from send_message is {response}")

response = client.send_message(
  QueueName = queueName,
  MessageBody = json.dumps(myMessage3),
  Priority = 50
)

sentMsgCount = 3

for i in range(20):
  response = client.send_message(
    QueueName = queueName,
    MessageBody = json.dumps(myMessage3),
    Priority = 50
  )
  sentMsgCount += 1
  log.info(f"Sent msg count: {sentMsgCount}")

