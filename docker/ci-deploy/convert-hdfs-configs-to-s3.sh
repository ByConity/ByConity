#!/bin/bash

# Need https://github.com/mikefarah/yq v4 to run this script.

cd "$(dirname "$0")" || exit

set -ex

PATCH_CMD='del (.hdfs_addr) 
| del (.storage_configuration.disks.hdfs_disk) 
| del (.storage_configuration.policies.default.volumes.hdfs)
| (. head_comment="Auto-generated! Please do not modify this file directly. Refer to '\''convert-hdfs-configs-to-s3.sh'\''.")
| . *= load("./s3/server-patch.yml")'

yq eval "${PATCH_CMD}" ./config/server.yml >./s3/server.yml
yq eval "${PATCH_CMD}" ./config/worker.yml >./s3/worker.yml
yq eval "${PATCH_CMD}" ./config/tso.yml >./s3/tso.yml
yq eval "${PATCH_CMD}" ./config/daemon-manager.yml >./s3/daemon-manager.yml

PATCH_CMD='del (.services.hdfs)
| del (.services.hdfs-datanode)
| (. head_comment="Auto-generated! Please do not modify this file directly. Refer to '\''convert-hdfs-configs-to-s3.sh'\''.")
| . *= load("./s3/docker-compose-patch.yml")'

yq eval "${PATCH_CMD}" ./docker-compose.yml >./docker-compose-s3.yml

cp ./config/users.yml ./s3/
