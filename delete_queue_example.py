import dqs 
import json

# Create a client

client = dqs.client()

# Queue deletion
queueName = 'MyNewQueue2'

client.delete_queue(
  QueueName = queueName,
)
