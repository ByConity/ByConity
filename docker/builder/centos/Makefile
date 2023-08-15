UID := $(shell id -u)
GID := $(shell id -g)
build: image
	mkdir -p $(HOME)/.ccache
	docker run --network=host --user=$(UID):$(GID) --rm --workdir /server --volume $(realpath ../../..):/server --cap-add=SYS_PTRACE --mount=type=bind,source=$(HOME)/.ccache,destination=/ccache -e CCACHE_DIR=/ccache -it byconity/byconity-builder-centos:stable

pull:
	docker pull byconity/byconity-builder-centos:stable

image:
	test -f foundationdb-clients-7.1.27-1.el7.x86_64.rpm || curl -L -o foundationdb-clients-7.1.27-1.el7.x86_64.rpm https://github.com/apple/foundationdb/releases/download/7.1.27/foundationdb-clients-7.1.27-1.el7.x86_64.rpm
	test -f cmake-3.25.3-linux-x86_64.sh || curl -L -o cmake-3.25.3-linux-x86_64.sh https://github.com/Kitware/CMake/releases/download/v3.25.3/cmake-3.25.3-linux-x86_64.sh || chmod +x cmake-3.25.3-linux-x86_64.sh
	test -f ninja-linux.zip || curl -L -o ninja-linux.zip https://github.com/ninja-build/ninja/releases/download/v1.11.1/ninja-linux.zip
	test -f llvm-project-11.1.0.src.tar.xz || curl -L -o llvm-project-11.1.0.src.tar.xz https://github.com/llvm/llvm-project/releases/download/llvmorg-11.1.0/llvm-project-11.1.0.src.tar.xz 

	docker build --network=host -t byconity/byconity-builder-centos .

image_push:
	docker tag byconity/byconity-builder-centos:latest byconity/byconity-builder-centos:stable
	docker push byconity/byconity-builder-centos:stable
