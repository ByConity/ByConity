## About
Debian image with byconity dependencies and common operational tools.
The foundationdb version is 7.1.27 (without avx supports).
If you need avx, changes the foudationdb to 7.1.26 (or any version from release page) and rebuild the image.
To build the image, run:

## Build Debian builder
```bash
DOCKER_BUILDKIT=1 docker build -t byconity/debian-builder \
  --build-arg http_proxy="${http_proxy}" \
  --build-arg https_proxy="${https_proxy}" \
  --target debian-builder .
```

Build Debian runner
```bash
DOCKER_BUILDKIT=1 docker build -t byconity/debian-runner \
  --build-arg http_proxy="${http_proxy}" \
  --build-arg https_proxy="${https_proxy}" \
  --target debian-runner .
```