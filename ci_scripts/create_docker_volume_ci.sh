docker volume rm my_volume
docker container create --name dummy -v my_volume:/opt/byconity/bin/ hello-world
mv $GITHUB_WORKSPACE/build/programs $GITHUB_WORKSPACE/build/bin
docker cp $GITHUB_WORKSPACE/build/bin dummy:/opt/byconity/
docker rm dummy
mv $GITHUB_WORKSPACE/build/bin $GITHUB_WORKSPACE/build/programs

docker volume rm my_hdfs_volume
docker container create --name dummy -v my_hdfs_volume:/etc/hadoop/conf/ hello-world
mv hdfs conf
docker cp /CI/conf/ dummy:/etc/hadoop/
docker rm dummy
mv conf hdfs

docker volume rm my_config_volume
docker container create --name dummy -v my_config_volume:/config hello-world
mv multi-workers config
docker cp /CI/config/ dummy:/
docker rm dummy
mv config multi-workers