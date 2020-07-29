import dqs
import json
from time import sleep

# Create a client
client = dqs.client()

# Queue name
# queueName='AWSBlog-shipment-demo'
queueName = 'MyNewQueue2'

client.open_queue(
  QueueName = queueName,
)

# Simple Consumer
messageCount = 0
while True:
  print(f"Polling message queue...  Messages received so far: {messageCount}")
  response = client.receive_message(
    QueueName = queueName,
  )
  print(f"Response from receive_message is {response}")
  if response == []:
    print('Sleeping 5s')
    sleep(5)
  else:
    messageCount += 1
    print(f'Processing message {messageCount}...')
    # simulate some message processing time
    sleep(0.100)
