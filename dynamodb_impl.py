from uuid import uuid4
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import dateutil.parser as dateparser
import boto3
from botocore import exceptions
import attr
import json
import logging
import logconfig
import os
from time import sleep

log = logging.getLogger(__name__)

DEFAULT_PRIORITY = 100

if  'AWS_LOCAL' in os.environ:
    DYNAMODB_ENDPOINT = os.environ['DYNAMODB_LOCAL_ENDPOINT']
    log.info("Using local endpoint %s", DYNAMODB_ENDPOINT)
    dynamodb = boto3.resource('dynamodb', endpoint_url=DYNAMODB_ENDPOINT, region_name='us-east-1')
    dynamodb_client = boto3.client('dynamodb', endpoint_url=DYNAMODB_ENDPOINT, region_name='us-east-1')
else:
    dynamodb = boto3.resource('dynamodb')
    dynamodb_client = boto3.client('dynamodb')


@attr.s
class SystemInfo():
    queue_selected = attr.ib(default=False)
    queued = attr.ib(default=0)
    version = attr.ib(default=1)
    status = attr.ib(default='READY_TO_ENQUEUE')
    queue_add_timestamp = attr.ib(default='0')
    queue_peek_timestamp = attr.ib(default='0')
    queue_remove_timestamp = attr.ib(default='0')
    dlq_add_timestamp = attr.ib(default='0')
    creation_timestamp = attr.ib(default='0')
    last_updated_timestamp = attr.ib(default='0')
    id = attr.ib(default='')
    priority = attr.ib(default='')

    def setQueue(self, queue_name):
        self.queue_selected = queue_name

    def setStatus(self, status):
        self.status = status


def ensure_cls(cl):     
    """If the attribute is an instance of cls, pass, else try constructing."""     
    def converter(val):         
        if isinstance(val, cl):             
            return val         
        else:             
            return cl(**val) 
    return converter


@attr.s
class Message():
    id = attr.ib()
    MessageBody = attr.ib()
    Priority = attr.ib()
    ReceiptHandle = attr.ib()
    last_updated_timestamp = attr.ib(default='0')
    system_info = attr.ib(default={}, converter=ensure_cls(SystemInfo))

    def __attrs_post_init__(self):
        self.set_id(self.id)
        self.system_info.priority = self.Priority

    def set_id(self, id):
        self.id = id
        self.system_info.id = id

@attr.s
class DynamoDbQueueConfig():
    VISIBILITY_TIMEOUT_IN_S = attr.ib(default=60)


class DynamoDbQueue():
    def __init__(self, tableName, config=DynamoDbQueueConfig()):
        self.actualTableName = tableName
        self.table = dynamodb.Table(tableName)
        self.config = config

    def getQueueStats(self):
        pass

    def getDLQStats(self):
        pass

    def get(self, id):
        try:
            response = self.table.get_item(
                Key={
                    'id': id,
                }
            )
            item = response['Item']
        except:
            item = None
        return item

    def put(self, item):
        response = self.table.put_item(
            Item=item
        )

    def updateStatus(self, id, newStatus):
        ''' Method for changing the status of the record 
	        This call should not be used unless there are operational issues and there are live issues that needs to be resolved. '''
        message = self.get(id)
        if message:
            if message.system_info.status == newStatus:
                return message
            odt = datetime.utcnow()
            message.system_info.status = newStatus
            try: 
                outcome = self.table.update_item(
                    updateExpresion="",
                    conditionExpression=Attr('version').eq('myvalue')
                )

            except:
                pass
                         
        return message

    def enqueue(self, id):
        message = self.get(id)
        outcome = None
        if message:
            status = message['system_info']['status']  
            version = int(message['system_info']['version'])
            log.debug("version is: %s", version)
            if status != 'READY_TO_ENQUEUE':
                raise Exception('Message to enqueue is in wrong state')
            now = datetime.utcnow().isoformat()+'Z'
            priority = int(message['system_info']['priority'])
            time_priority = datetime.fromtimestamp(datetime.utcnow().timestamp()+86400*priority)
            priority_timestamp = time_priority.isoformat()+'Z'
            try:
                outcome = self.table.update_item(
                    Key = {
                        'id': id
                    },
                    UpdateExpression = "ADD #sys.#v :one "
                        + "SET queued = :one, #sys.queued = :one, #sys.queue_selected = :false, "
                        + "last_updated_timestamp = :pri, #sys.last_updated_timestamp = :pri, "
                        + "#sys.queue_added_timestamp = :lut, #sys.#st = :st",
                    ConditionExpression=Attr('system_info.version').eq(version), 
                    ExpressionAttributeNames={
                        '#v':'version', 
                        '#st':'status', 
                        '#sys':'system_info'
                    },
                    ExpressionAttributeValues={
                        ':one': 1, 
                        ':false': False,
                        ':st': 'ENQUEUED', 
                        ':lut': now,
                        ':pri': priority_timestamp
                    }
                )
            except exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    log.info('Enqueue update blocked by optimistic locking version number.')
                else:
                    log.error('Enqueue update failed.  Exception: %s', e)
            except Exception as e:
                log.error('Enqueue update failed.  Exception: %s', e)
        else:
            raise Exception(f"Unknown message id passed to enqueue: {id}")

        return outcome


    def peek(self):
        selectedID = None
        selectedVersion = 0
        recordForPeekIsFound = False
        exclusiveStartKey = None
        done = False
        receivedMessage = None

        while not done:
            # Workaround issue with ExclusiveStartKey in boto3:  https://github.com/boto/botocore/issues/1688
            if exclusiveStartKey == None:
                queryResult = self.table.query(
                    IndexName = 'queueud-last_updated_timestamp-index',
                    Limit=2, # 250
                    ConsistentRead=False,
                    ScanIndexForward=True,
                    ProjectionExpression='id, queued, system_info',
                    KeyConditionExpression=Key('queued').eq(1),
                )
            else:
                log.debug("Executing query with ExclusiveStartKey %s", exclusiveStartKey)
                queryResult = self.table.query(
                    IndexName = 'queueud-last_updated_timestamp-index',
                    Limit=2, # 250
                    ConsistentRead=False,
                    ScanIndexForward=True,
                    ProjectionExpression='id, queued, system_info',
                    KeyConditionExpression=Key('queued').eq(1),
                    ExclusiveStartKey=exclusiveStartKey
                )        

            if 'LastEvaluatedKey' in queryResult:
                exclusiveStartKey = queryResult['LastEvaluatedKey'] 
            else:
                exclusiveStartKey = None

            for message in queryResult['Items']:
                if 'queue_selected' in message['system_info'] and message['system_info']['queue_selected']:
                    currentTS = int(datetime.utcnow().timestamp())
                    lastPeekTimeUTC = int(message['system_info']['peek_utc_timestamp'])
                    diff = currentTS - lastPeekTimeUTC

                    if currentTS - lastPeekTimeUTC > self.config.VISIBILITY_TIMEOUT_IN_S:
                        selectedID = message['system_info']['id']
                        selectedVersion = message['system_info']['version']
                        # Converted straggler.
                        recordForPeekIsFound = True
                        log.debug("Converted straggler version: %s", selectedVersion)
                        break
                
                else:
                    selectedID = message['system_info']['id']
                    selectedVersion = message['system_info']['version']
                    recordForPeekIsFound = True
                    log.debug("Selected message id: %s version: %i", selectedID, selectedVersion)
                    break
			
            # We're done if we found a message to peek at, or no more pages of messages:
            done = recordForPeekIsFound == True or exclusiveStartKey == None

        if selectedID == None:
            # Queue is empty
            log.debug("Queue is empty")
            return None
        
        log.debug("MESSAGE ID TO PEEK: %s.  SELECTED VERSION: %s", selectedID, selectedVersion)

        message = self.get(selectedID)
        id = message['id']
        now = self.now()
        tsUTC = int(datetime.utcnow().timestamp())

        try:
            outcome = self.table.update_item(
                Key = {
                    'id': id
                },
                UpdateExpression = "ADD #sys.#v :one "
                    + "SET #sys.queue_selected = :true, "
                	+ "#sys.last_updated_timestamp = :lut, "
                	+ "#sys.queue_peek_timestamp = :lut, "
                	+ "#sys.peek_utc_timestamp = :ts, #sys.#st = :st",
                ConditionExpression=Attr('system_info.version').eq(selectedVersion), 
                ExpressionAttributeNames={
                    '#v':'version', 
                    '#st':'status', 
                    '#sys':'system_info'
                },
                ExpressionAttributeValues={
                    ':one': 1, 
                    ':true': True,
                    ':st': 'PROCESSING', 
                    ':lut': now,
                    ":ts": tsUTC
                }
            )
            receivedMessage = self.get(selectedID)
        except exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # This is an expected exception when multiple consumers are competing for messages.
                log.info('Enqueue update blocked by optimistic locking version number.')
            else:
                log.warning('Enqueue update blocked by ClientError exception: %s', e)
            receivedMessage = 'retry'
        except Exception as e:
            log.warning('Peek blocked by Exception: %s', e)
            receivedMessage = 'retry'
        return receivedMessage


    def dequeue(self):
        result = self.peek()
        if result:
            id = result.getId()
            removeResult = self.remove(id)
            # If remove failed, don't return the peeked message
            if not removeResult:
                result = None
        return result

    def remove(self, id):
        try:
            response = self.table.delete_item(
                Key = {
                    'id': id
                }
            )
        except:
            pass

    def sendToDLQ(self):
        pass

    def deleteQueue(self):
        try:
            response = self.table.delete()
        except:
            pass

    def now(self):
        return datetime.utcnow().isoformat()+'Z'
    

class DynamoDbTableCreator():

    def __init__(self, client):
        self.dynamodb_client = client

    def tableExists(self, tableName):
        try:
            result = self.dynamodb_client.describe_table(TableName=tableName)
            return True
        except:
            return False

    def delayUntilTableExists(self, tableName):
        while not self.tableExists(tableName):
            log.info('Waiting for queue %s', tableName)
            sleep(2)
        
    def createTable(self, tableName):
        try:
            result = dynamodb_client.create_table(
                TableName=tableName,
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'last_updated_timestamp',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'queued',
                        'AttributeType': 'N'
                    },
                ],
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'
                    },
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'queueud-last_updated_timestamp-index',
                        'KeySchema': [
                            {
                                'AttributeName': 'queued',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'last_updated_timestamp',
                                'KeyType': 'RANGE'
                            },
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL',
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 100,
                            'WriteCapacityUnits': 100
                        }
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 100,
                    'WriteCapacityUnits': 100
                },
            )
            log.info("Creating queue %s", tableName)
        except Exception as e:
            log.error("Could not create DynamoDB table %s.  Exception: %s", tableName, e)
        return result

class DynamoDbImpl():

    def __init__(self):
        self.dynamoDbQueues = {}
        self.tableCreator = DynamoDbTableCreator(dynamodb_client)

    def create_queue(self, QueueName, QueueType='priority'):
        log.info('%s: Creating %s queue', QueueName, QueueType)
        # Create new Table (if necessary) to represent this queue
        if not self.tableCreator.tableExists(QueueName):
            log.info('Creating new table for queue %s', QueueName)
            self.tableCreator.createTable(QueueName)
            self.tableCreator.delayUntilTableExists(QueueName)
        self.dynamoDbQueues[QueueName] = DynamoDbQueue(QueueName)
        return True

    def open_queue(self, QueueName):
        # See if Table exists for this queue
        if not self.tableCreator.tableExists(QueueName):
            self.tableCreator.delayUntilTableExists(QueueName)
        log.info('%s: Opening queue', QueueName)
        self.dynamoDbQueues[QueueName] = DynamoDbQueue(QueueName)
        return True

    def delete_queue(self, QueueName):
        # Delete Table associated with this queue
        log.info('%s: Deleting queue', QueueName)
        queue = DynamoDbQueue(QueueName)
        queue.deleteQueue()
        return True

    def send_message(self, QueueName, MessageBody, Priority=DEFAULT_PRIORITY):
        # Do a put, then enqueue
        id = None
        log.info('%s: sending message with priority %i', QueueName, Priority)
        log.info('%s: MessageBody: %s', QueueName, MessageBody)
        if QueueName in self.dynamoDbQueues:
            id = str(uuid4())
            msg = Message(id=id, MessageBody=MessageBody, Priority=Priority, ReceiptHandle=id)
            log.debug("Message being sent is: %s", attr.asdict(msg))
            self.dynamoDbQueues[QueueName].put(attr.asdict(msg))
            self.dynamoDbQueues[QueueName].enqueue(id)
        else:
            raise Exception(f"Invalid QueueName in send_message: {QueueName}")
        return id

    def receive_message(self, QueueName):
        # peek the message queue
        log.debug('%s: receiving messages via DynamoDB', QueueName)
        result = []
        if QueueName in self.dynamoDbQueues:
            # TODO: Add support for receiving in batches
            message = self.dynamoDbQueues[QueueName].peek()
            while message == 'retry':
                message = self.dynamoDbQueues[QueueName].peek()
            if message:
                result = [message]
        return result

    def delete_message(self, QueueName, ReceiptHandle):
        # Do a dequeue / remove 
        log.debug('%s: deleting a message with ReceiptHandle %s', QueueName, ReceiptHandle)
        if QueueName in self.dynamoDbQueues:
            self.dynamoDbQueues[QueueName].remove(ReceiptHandle)
        return True
