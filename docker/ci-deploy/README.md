The role of file and subdirectory in this directory:
- docker-compose.yml: the docker compose file for running CI test with HDFS storage. So it contains the containers for HDFS storage. 
- docker-compose-s3.yml: the docker compose file for running CI test with S3-API-compatibled storage. It using the minio docker image for 
- docker-compose-external-test.yml : the docker compose file for running external test suite. The test suite is for testing features that communicate with external service like Kafka, Community Clickhouse.
