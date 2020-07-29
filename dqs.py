from dynamodb_impl import DynamoDbImpl
import logging
import logconfig

log = logging.getLogger(__name__)

DEFAULT_PRIORITY = 100

class DqsMock():
  def create_queue(self, QueueName, QueueType='priority'):
    log.info(f'{QueueName}: Creating {QueueType} queue')
    return True

  def open_queue(self, QueueName):
    log.info(f'{QueueName}: Opening queue')
    return True

  def delete_queue(self, QueueName):
    log.info(f'{QueueName}: Deleting queue')
    return True

  def send_message(self, QueueName, MessageBody, Priority=DEFAULT_PRIORITY):
    log.info(f'{QueueName}: sending message with priority {Priority}')
    log.info(f'{QueueName}: MessageBody: {MessageBody}')
    return '42'

  def receive_message(self, QueueName):
    log.info(f'{QueueName}: receiving messages')
    return []

  def delete_message(self, QueueName, ReceiptHandle):
    log.info(f'{QueueName}: deleting a message with ReceiptHandle {ReceiptHandle}')
    return True


class DqsClient():
  def __init__(self, impl):
    self.impl = impl

  def create_queue(self, QueueName, QueueType):
    return self.impl.create_queue(QueueName, QueueType)

  def open_queue(self, QueueName):
    return self.impl.open_queue(QueueName)

  def delete_queue(self, QueueName):
    return self.impl.deleted_queue(QueueName)

  def send_message(self, QueueName, MessageBody, Priority=DEFAULT_PRIORITY):
    return self.impl.send_message(QueueName=QueueName, MessageBody=MessageBody, Priority=Priority)

  def receive_message(self, QueueName):
    return self.impl.receive_message(QueueName)

  def delete_message(self, QueueName, ReceiptHandle):
    return self.impl.delete_message(QueueName, ReceiptHandle)

def client(impl = DynamoDbImpl()):
  return DqsClient(impl)


class DqsQueue():
  def __init__(self, QueueName):
    self.QueueName = QueueName
    self.client = client()
    self.client.open_queue(QueueName)

  def send_message(self, MessageBody, Priority=DEFAULT_PRIORITY):
    return self.client.send_message(self.QueueName, MessageBody, Priority)

  def receive_message(self):
    return self.client.receive_message(self.QueueName)

  def delete_message(self, ReceiptHandle):
    return self.client.delete_message(self.QueueName, ReceiptHandle)


def queue(QueueName):
  return DqsQueue(QueueName)

