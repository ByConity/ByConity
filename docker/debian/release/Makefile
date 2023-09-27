BYCONITY_SOURCE=$(realpath ../../..)
BUILD_TYPE ?= "Release"
CMAKE_FLAGS ?= ""
TAG ?= "0.2.1"

pull:
	docker pull byconity/debian-builder
	docker pull byconity/debian-runner

compile:
	docker run --rm \
	--privileged --cap-add SYS_PTRACE \
	-v ~/.m2:/root/.m2 \
	-v ${BYCONITY_SOURCE}:/root/ByConity \
	-e BUILD_TYPE=${BUILD_TYPE} -e CMAKE_FLAGS=${CMAKE_FLAGS} -e VERSION_SCM=${TAG} \
	-it byconity/debian-builder bash /build.sh

image:
	cp -r ${BYCONITY_SOURCE}/build_install .
	DOCKER_BUILDKIT=1 docker build -t byconity/byconity:${TAG} .

release:
	docker push byconity/byconity:${TAG}
