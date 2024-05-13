# Prepare Hudi data

```bash
docker-compose up

wget -P scripts https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/load/hudi/hudi_docker_compose_attached_file.zip

unzip scripts/hudi_docker_compose_attached_file.zip -d scripts

docker-compose exec -it adhoc-1 /bin/bash /var/scripts/setup_demo_container_adhoc_1.sh
docker-compose exec -it adhoc-2 /bin/bash /var/scripts/setup_demo_container_adhoc_2.sh

# connect to hive metastore
docker-compose exec -it adhoc-1 hive

```
