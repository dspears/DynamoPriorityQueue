import dqs
import json

# Create a client
client = dqs.client()

# Queue name
queueName='AWSBlog-shipment-demo'
client.create_queue(
  QueueName = queueName,
  QueueType = 'priority'
)
# Consumer
response = client.receive_message(
  QueueName = queueName,
)

print(f"Response from receive_message is {response}")
