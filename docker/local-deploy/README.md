## Byconity local development

Byconity includes many external components, so the easiest way for local development is to use docker-compose. Here provide a step-by-step guide so you can test your local Byconity build.

Prerequisite:
- a Linux environment (preferably Ubuntu, Debian)
- docker
- docker-compose
- a docker runtime is up and running

### Change the `.env` file to match your development environment

Set `BYCONITY_BINARY_PATH` to the local byconity binaries path

```
# replace to Byconity binaries path
BYCONITY_BINARY_PATH=/data01/{user}/cnch_build2/build_byconity/programs/
```

### Bring the cluster up

Run:

```
docker-compose up -d
```

This will create a local cluster with basic byconity components, hdfs, and foundationdb. If you want to run byconity with resource-manager and multiple read workers, use:

```
docker-compose -f docker-compose.yml.multiworkers up -d
```

### Create hdfs users
Internally, byconity read/write to hdfs with username `clickhouse` (and data is stored in `/user/clickhouse/`), which is not created by default when starting hadoop cluster. We can use following commands to create the user `clickhouse` on hdfs.

```
./hdfs/create_users.sh
```

### Connect to the cluster

You can either use your local build `clickhouse` binary or use official `clickhouse` client to connect to byconity. To install offical `clickhouse-client`, run:

```
sudo apt-get install -y apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754

echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client
```

Then connect to your byconity cluster by:

```
clickhouse client
```

### Troubleshooting

TBD;
