UID := $(shell id -u)
GID := $(shell id -g)
BYCONITY_SOURCE := $(shell pwd)/../../..
NAME := ${UID}-dev-env

run:
	mkdir -p ~/.ccache
	docker run -it \
		--privileged \
		--cap-add SYS_PTRACE \
		--user=${UID}:${GID} \
		--network host \
		-e CCACHE_DIR=/root/.ccache \
		-v ~/.m2:/root/.m2 \
		-v ${BYCONITY_SOURCE}:/root/ByConity \
		-v ~/.ccache:/root/.ccache \
		--name ${NAME} \
		--workdir /root/ByConity \
		-d byconity/dev-env

pull:
	docker pull byconity/dev-env

exec:
	docker exec -it ${NAME} zsh

stop:
	docker stop ${NAME}
