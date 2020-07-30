# DynamoDB Queues

Execute the following to try locally using DynamoDB Local container, and 3 consumer/worker nodes:

- docker-compose build
- docker-compose up -d --scale worker-node=3

Watch logs of the three workers:

- docker logs -f dynamodb_worker-node_1
- docker logs -f dynamodb_worker-node_2
- docker logs -f dynamodb_worker-node_3

Log in to the producer container:

- docker exec -it dynamodb_producer_1 /bin/bash

Send messages from the producer:

- python send_example.py

Consumer timing intervals can be controlled by the env vars in docker-compose.yml:

- POLL_INTERVAL=4
- PROCESSING_TIME=0.110

Poll interval is time in seconds between queries to DynamoDB.

Processing time is simulated time to process a message.
