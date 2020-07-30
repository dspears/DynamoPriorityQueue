import dqs
import json

# Create a client
client = dqs.client()

# Queue name
# queueName='AWSBlog-shipment-demo'
queueName = 'MyNewQueue4'

client.open_queue(
  QueueName = queueName,
)

# Consumer
response = client.receive_message(
  QueueName = queueName,
)

print(f"Response from receive_message is {response}")
