import dqs
import json
from time import sleep
import logging
import logconfig
import os
log = logging.getLogger(__name__)

if 'POLL_INTERVAL' in os.environ:
  poll_interval = float(os.environ['POLL_INTERVAL'])
else:
  poll_interval = 5

if 'PROCESSING_TIME' in os.environ:
  processing_time = float(os.environ['PROCESSING_TIME'])
else:
  processing_time = 0.100

log.info(f'Consumer starting - poll interval: {poll_interval}s, processing time: {processing_time}s')

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
  response = client.receive_message(
    QueueName = queueName,
  )
  if response == []:
    sleep(poll_interval)
  else:
    id = response[0]['id']
    messageCount += 1
    log.info(f"Messages received: {messageCount} id: {id}")
    # simulate some message processing time
    sleep(processing_time)
    receiptHandle = response[0]['ReceiptHandle']
    client.delete_message(
      QueueName = queueName,
      ReceiptHandle = receiptHandle
    )

