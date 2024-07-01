# ByConity Docker Compose

## 1. Create an Environment File:

Start by creating an environment file named .env from the provided template .env.tpl.
```bash
cp .env.tpl .env
```

## 2. Modify the Environment Variables:
Modify .env file accordingly. Here are the variables you can configure:

* `COMPOSE_PROJECT_NAME`: This sets the project name for your Docker Compose setup.
* `DATA_DIR`: Specify the local path that will be mounted into the Docker containers. This path is used for local disk cache, logs, etc.
* `SERVER_TCP_PORT` and `SERVER_HTTP_PORT`: These variables define the exposed ports for the ByConity server's TCP and HTTP ports.

### Running a Specific Version of ByConity:

If you want to run a specific release version of ByConity
1. Update the BYCONITY_IMAGE variable in the .env file to the desired release version, e.g., byconity/byconity:0.2.0.
2. Set BYCONITY_BINARY_PATH to an empty string.

### Running ByConity with a Locally Built Binary

If you want to run ByConity with a locally built binary
1. Update the BYCONITY_IMAGE variable in the .env file to byconity/debian-runner.
2. Set BYCONITY_BINARY_PATH to the path where your locally built binary resides.

You can compile the project with ByConity [dev-env](../debian/dev-env/README.md) docker image

## Starting ByConity with Docker Compose
To start ByConity using Docker Compose, use the following command:
```bash
docker-compose -f docker-compose.essentials.yml [-f docker-compose.simple.yml] up [-d]
```

The -d option runs the containers in the background.
* docker-compose.essentials.yml: esential dependencies to start byconity cluster
* docker-compose.simple.yml: simple byconity cluster
* docker-compose.multiworkers.yml: multi-worker byconity cluster

To access the ByConity cluster with cli
```bash
./scripts/byconity-cli.sh
```

To access HDFS files
```bash
./scripts/hdfs-cli.sh "hdfs dfs -ls /user/clickhouse"
```

To access fdb cli
```bash
./scripts/fdb-cli.sh
```

## Printing Logs to the Console
To print a service's logs to the console, you need to update the configuration file under `byconity-*-cluster`. Set `logger: console: true` in the configuration file.

# Integrate with Hive
```bash
docker-compose -f docker-compose.hive.yml -d up

./scripts/beeline.sh
```

# Integrate with Hudi
