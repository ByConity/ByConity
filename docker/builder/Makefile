build: image
	mkdir -p $(HOME)/.ccache
	docker run --network=host --rm --workdir /server --volume $(realpath ../..):/server --cap-add=SYS_PTRACE --mount=type=bind,source=$(HOME)/.ccache,destination=/ccache -e CCACHE_DIR=/ccache -it minhthucdao1/byconity-builder

pull:
	docker pull minhthucdao1/byconity-builder

image:
	docker build --network=host -t minhthucdao1/byconity-builder .

image_push:
	docker push minhthucdao1/byconity-builder
