import dqs
import json
from time import sleep
import logging
import logconfig
log = logging.getLogger(__name__)

# Create a client
client = dqs.client()

# Queue name
# queueName='AWSBlog-shipment-demo'
queueName = 'MyNewQueue5'

client.open_queue(
  QueueName = queueName,
)

# Simple Consumer
messageCount = 0
while True:
  log.info(f"Polling message queue...  Messages received so far: {messageCount}")
  response = client.receive_message(
    QueueName = queueName,
  )
  log.info(f"Response from receive_message is {response}")
  if response == []:
    log.info('Sleeping 5s')
    sleep(5)
  else:
    messageCount += 1
    log.info(f'Processing message {messageCount}...')
    # simulate some message processing time
    sleep(0.100)
    id = response[0]['id']
    receiptHandle = response[0]['ReceiptHandle']
    log.info(f'NOT Deleteing message {id}')
    # client.delete_message(
    #   QueueName = queueName,
    #   ReceiptHandle = receiptHandle
    # )
