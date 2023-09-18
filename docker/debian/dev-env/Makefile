BYCONITY_SOURCE := $(realpath ../../..)
CONTAINER := $(shell id -u)-dev-env

run:
	mkdir -p $(HOME)/.ccache
	docker run -it --rm \
		--privileged \
		--cap-add SYS_PTRACE \
		--network host \
		-e CCACHE_DIR=/root/.ccache \
		-e http_proxy=${http_proxy} \
		-e https_proxy=${https_proxy} \
		-v $(HOME)/.m2:/root/.m2 \
		-v $(HOME)/ck:/root/ck \
		-v $(HOME)/.ccache:/root/.ccache \
		--name $(CONTAINER) \
		--workdir /root/ByConity \
		byconity/dev-env bash

pull:
	docker pull byconity/dev-env
