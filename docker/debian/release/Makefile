BYCONITY_SOURCE=$(realpath ../../..)
BINARY_DIR = ${BIN_DIR}
BUILD_TYPE ?= "Release"
CMAKE_FLAGS ?= ""
TAG ?= "latest"
UID := $(shell id -u)
GID := $(shell id -g)

pull:
	docker pull byconity/debian-builder
	docker pull byconity/debian-runner

compile:
	docker run --rm --user=$(UID):$(GID) \
	--cap-add SYS_PTRACE \
	--mount=type=bind,source=$(HOME)/.ccache,destination=/ccache \
	-v ~/.m2:/home/.m2 \
	-v ${BYCONITY_SOURCE}:/home/ByConity \
	-e BUILD_TYPE=${BUILD_TYPE} -e CMAKE_FLAGS=${CMAKE_FLAGS} -e VERSION_SCM=${TAG} -e CCACHE_DIR=/ccache \
	-it byconity/debian-builder bash /build.sh

image:
	cp -r ${BYCONITY_SOURCE}/build_install .
	DOCKER_BUILDKIT=1 docker build -t byconity/byconity:${TAG} .

image-github:
	mkdir -p build_install
	cp -ra ${BINARY_DIR}/usr/* ./build_install/
	cp -ra ${BINARY_DIR}/etc ./build_install/
	DOCKER_BUILDKIT=1 docker build -t byconity/byconity:${TAG} .

release:
	docker push byconity/byconity:${TAG}
test:
	docker run --rm -it byconity/byconity /bin/bash
