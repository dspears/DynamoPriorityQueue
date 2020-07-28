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

print(f"Response from send_message is {response}")
