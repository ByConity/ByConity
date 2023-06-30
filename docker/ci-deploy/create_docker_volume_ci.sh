docker container create --name dummy -v ${BINARY_VOL}:/opt/byconity/bin/ hello-world
docker cp ./bin dummy:/opt/byconity/
docker rm dummy

docker container create --name dummy -v ${HDFS_VOL}:/etc/hadoop/conf/ hello-world
docker cp /CI/conf/ dummy:/etc/hadoop/
docker rm dummy

docker container create --name dummy -v ${CONFIG_VOL}:/config hello-world
docker cp /CI/config/ dummy:/
docker rm dummy

docker container create --name dummy -v ${SCRIPTS_VOL}:/mnt/scripts hello-world
docker cp /CI/hive/scripts/ dummy:/mnt/
docker rm dummy
