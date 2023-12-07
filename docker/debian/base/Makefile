UID := $(shell id -u)
GID := $(shell id -g)

test_builder:
	docker run --network=host --user=$(UID):$(GID) --rm -it byconity/debian-builder /bin/bash

test_runner:
	docker run --network=host --user=$(UID):$(GID) --rm -it byconity/debian-runner /bin/bash

builder:
	DOCKER_BUILDKIT=1 docker build -t byconity/debian-builder \
  --build-arg http_proxy="${http_proxy}" \
  --build-arg https_proxy="${https_proxy}" \
  --target debian-builder .
runner:
	DOCKER_BUILDKIT=1 docker build -t byconity/debian-runner \
  --build-arg http_proxy="${http_proxy}" \
  --build-arg https_proxy="${https_proxy}" \
  --target debian-runner .
push_builder:
	docker tag byconity/debian-builder byconity/debian-builder:latest
	docker push byconity/debian-builder:latest
push_runner:
	docker tag byconity/debian-runner byconity/debian-runner:latest
	docker push byconity/debian-runner:latest


