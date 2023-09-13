## About
Debian image with byconity dependencies and common operational tools.
The foundationdb version is 7.1.27 (without avx supports).
If you need avx, changes the foudationdb to 7.1.26 (or any version from release page) and rebuild the image.
To build the image, run:

## ByConity dev-env image
```
BYCONITY_SOURCE=../../
docker run -it --privileged --cap-add SYS_PTRACE \
  -v ~/.m2:/root/.m2 \
  -v ${BYCONITY_SOURCE}:/root/ByConity \
  --name byconity-dev \
  -d byconity/debian-builder


docker exec -it byconity-dev /bin/bash
```

## Build Debian builder
```bash
DOCKER_BUILDKIT=1 docker build -t byconity/debian-builder \
  --build-arg http_proxy="${http_proxy}" \
  --build-arg https_proxy="${https_proxy}" \
  --target builder .
```

Build Debian runner
```bash
DOCKER_BUILDKIT=1 docker build -t byconity/debian-runner \
  --build-arg http_proxy="${http_proxy}" \
  --build-arg https_proxy="${https_proxy}" \
  --target debian-runner .
```