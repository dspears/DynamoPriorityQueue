from uuid import uuid4
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import dateutil.parser as dateparser
import boto3
import attr
import json
import logging
import logconfig

log = logging.getLogger(__name__)

DEFAULT_PRIORITY = 100

dynamodb = boto3.resource('dynamodb')
# dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
dynamodb_client = boto3.client('dynamodb')
# dynamodb_client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')


@attr.s
class SystemInfo():
    queue_selected = attr.ib(default=False)
    queued = attr.ib(default=0)
    version = attr.ib(default=1)
    status = attr.ib(default='READY_TO_ENQUEUE')
    queue_add_timestamp = attr.ib(default=datetime.utcnow().isoformat()+'Z')
    queue_peek_timestamp = attr.ib(default=datetime.utcnow().isoformat()+'Z')
    queue_remove_timestamp = attr.ib(default=datetime.utcnow().isoformat()+'Z')
    dlq_add_timestamp = attr.ib(default=datetime.utcnow().isoformat()+'Z')
    creation_timestamp = attr.ib(default=datetime.utcnow().isoformat()+'Z')
    last_updated_timestamp = attr.ib(default=datetime.utcnow().isoformat()+'Z')
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
    last_updated_timestamp = attr.ib(default=datetime.utcnow().isoformat()+'Z')
    system_info = attr.ib(default={}, converter=ensure_cls(SystemInfo))

    def __attrs_post_init__(self):
        self.set_id(self.id)
        self.system_info.priority = self.Priority
        time_priority = datetime.fromtimestamp(datetime.utcnow().timestamp()-86400*self.Priority)
        self.system_info.last_updated_timestamp = time_priority.isoformat()+'Z'

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
        log.info("put item:", item)
        response = self.table.put_item(
            Item=item
        )
        log.info("put response is:", response)


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
            log.info('enqueue found message: ', message)
            status = message['system_info']['status']  
            version = int(message['system_info']['version'])
            log.info(f"version is: {version} of type {type(version).__name__}")
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
                log.info('enqueue did update_item', outcome)
            # except ConditionalCheckFailedException as e:
            #     log.info('Enqueue update blocked by optimistic locking version number.')
            except Exception as e:
                log.error('Enqueue update failed.  Exception:', e)
        else:
            raise Exception(f"Unknown message id passed to enqueue: {id}")

        return outcome


    def peek(self):
        selectedID = None
        selectedVersion = 0
        recordForPeekIsFound = False
        exclusiveStartKey = {}
        done = False
        receivedMessage = None

        while not done:
            queryResult = self.table.query(
                IndexName = 'queueud-last_updated_timestamp-index',
                Limit=250,
                ConsistentRead=False,
                ScanIndexForward=True,
                ProjectionExpression='id, queued, system_info',
                KeyConditionExpression=Key('queued').eq(1),
            )

            log.info("peek did query.  result:", queryResult)

            if 'LastEvaluatedKey' in queryResult:
                exclusiveStartKey = queryResult['LastEvaluatedKey'] 
            else:
                log.info('LastEvaluatedKey is not in queryResults')

            log.info(f"exclusiveStartKey is: {exclusiveStartKey}")

            for message in queryResult['Items']:
                # log.info(f"Message from query id {message['system_info']['id']} v: {message['system_info']['version']}")
                if 'queue_selected' in message['system_info'] and message['system_info']['queue_selected']:
                    currentTS = int(datetime.utcnow().timestamp())
                    lastPeekTimeUTC = int(message['system_info']['peek_utc_timestamp'])
                    diff = currentTS - lastPeekTimeUTC
                    # log.info(f"queue_selected: message['system_info']['queue_selected'] currentTS: {currentTS} lastPeekTimeUTC: {lastPeekTimeUTC} diff {diff}")

                    if currentTS - lastPeekTimeUTC > self.config.VISIBILITY_TIMEOUT_IN_S:
                        selectedID = message['system_info']['id']
                        selectedVersion = message['system_info']['version']
                        # Converted straggler.
                        recordForPeekIsFound = True
                        log.info(f"Converted straggler version: {selectedVersion}")
                        break
                
                else:
                    selectedID = message['system_info']['id']
                    selectedVersion = message['system_info']['version']
                    recordForPeekIsFound = True
                    log.info(f"Selected message id: {selectedID} version: {selectedVersion}")
                    break
			
            # We're done if we found a message to peek at, or no more pages of messages:
            done = recordForPeekIsFound == True or exclusiveStartKey == {}

        if selectedID == None:
            # Queue is empty
            log.info("Queue is empty")
            return None
        
        log.info(f"MESSAGE ID TO PEEK: {selectedID}.  SELECTED VERSION: {selectedVersion}")

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
            log.info('peek did update_item. outcome is:', outcome)
            receivedMessage = self.get(selectedID)
        # except ConditionalCheckFailedException as e:
        #     log.info('Peek blocked by optimistic locking version number.')
        except Exception as e:
            log.info('Peek blocked by optimistic locking version number.')
            receivedMessage = 'retry'
            # log.warning('Peek update failed.  Exception:', e)
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
        message = self.get(id)
        if message:
            return

    def sendToDLQ(self):
        pass

    def deleteQueue(self):
        pass

    def now(self):
        return datetime.utcnow().isoformat()+'Z'
    

class DynamoDbTableCreator():

    @staticmethod
    def tableExists(tableName):
        try:
            result = dynamodb_client.describe_table(TableName=tableName)
            return True
        except:
            return False

    @staticmethod
    def createTable(tableName):
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
            log.info(f"Created table {tableName}", result)
        except Exception as e:
            log.error(f"Could not create DynamoDB table {tableName}", e)
        return result

class DynamoDbImpl():

    def __init__(self):
        self.dynamoDbQueues = {}

    def create_queue(self, QueueName, QueueType='priority'):
        # Create new Table (if necessary) to represent this queue
        if not DynamoDbTableCreator.tableExists(QueueName):
            log.info(f'Creating new table for queue {QueueName}')
            DynamoDbTableCreator.createTable(QueueName)
        log.info(f'{QueueName}: Creating {QueueType} queue')
        self.dynamoDbQueues[QueueName] = DynamoDbQueue(QueueName)
        return True

    def open_queue(self, QueueName, QueueType='priority'):
        # See if Table exists for this queue
        if not DynamoDbTableCreator.tableExists(QueueName):
            raise Exception(f'DynamoDB table does not exist for queue {QueueName}')
        log.info(f'{QueueName}: Opening {QueueType} queue')
        self.dynamoDbQueues[QueueName] = DynamoDbQueue(QueueName)
        return True

    def delete_queue(self, QueueName):
        # Delete Table associated with this queue
        log.info(f'{QueueName}: Deleting queue')
        if QueueName in self.dynamoDbQueues:
            self.dynamoDbQueues[QueueName].deleteQueue()
            self.dynamoDbQueues.pop(QueueName)
        return True

    def send_message(self, QueueName, MessageBody, Priority=DEFAULT_PRIORITY):
        # Do a put, then enqueue
        id = None
        log.info(f'{QueueName}: sending message with priority {Priority}')
        log.info(f'{QueueName}: MessageBody: {MessageBody}')
        if QueueName in self.dynamoDbQueues:
            id = str(uuid4())
            msg = Message(id=id, MessageBody=MessageBody, Priority=Priority, ReceiptHandle=id)
            log.info(f"Message being sent is: {attr.asdict(msg)}")
            self.dynamoDbQueues[QueueName].put(attr.asdict(msg))
            self.dynamoDbQueues[QueueName].enqueue(id)
        else:
            raise Exception(f"Invalid QueueName in send_message: {QueueName}")
        return id

    def receive_message(self, QueueName):
        # peek the message queue
        log.info(f'{QueueName}: receiving messages via DynamoDB')
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
        log.info(f'{QueueName}: deleting a message with ReceiptHandle {ReceiptHandle}')
        if QueueName in self.dynamoDbQueues:
            self.dynamoDbQueues[QueueName].remove(ReceiptHandle)
        return True
