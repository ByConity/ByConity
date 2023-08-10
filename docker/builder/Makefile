UID := $(shell id -u)
GID := $(shell id -g)
build: pull 
	mkdir -p $(HOME)/.ccache
	docker run --network=host --user=$(UID):$(GID) --rm --workdir /server --volume $(realpath ../..):/server --cap-add=SYS_PTRACE --mount=type=bind,source=$(HOME)/.ccache,destination=/ccache -e CCACHE_DIR=/ccache -it byconity/byconity-builder

pull:
	docker pull byconity/byconity-builder

image:
	test -f foundationdb-clients_7.1.27-1_amd64.deb || curl -L -o foundationdb-clients_7.1.27-1_amd64.deb https://github.com/apple/foundationdb/releases/download/7.1.27/foundationdb-clients_7.1.27-1_amd64.deb
	docker build --network=host -t byconity/byconity-builder .

image_push:
	docker push byconity/byconity-builder

test:
	docker run --network=host --user=$(UID):$(GID) --rm -it byconity/byconity-builder /bin/bash

