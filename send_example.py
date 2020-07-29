import dqs  # i.e. Daupler DynamoDB Queue Service
import json

# Create a client

client = dqs.client()

# Queue creation
# queueName='AWSBlog-shipment-demo'
queueName = 'MyNewQueue2'

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

print(f"Response from send_message is {response}")

response = client.send_message(
  QueueName = queueName,
  MessageBody = json.dumps(myMessage2),
  Priority = 10
)

print(f"Response from send_message is {response}")

response = client.send_message(
  QueueName = queueName,
  MessageBody = json.dumps(myMessage3),
  Priority = 50
)

print(f"Response from send_message is {response}")
