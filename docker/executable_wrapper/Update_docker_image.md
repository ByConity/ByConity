Executable wrapper docker image is a wrapper of executable binary for ByConity. To update docker image for executable wrapper, following below steps:

Step 1: build executable binary for ByConity
- From root folder of git repo, rm folder `build_docker` if it exists
- cd to `docker/builder`
- Build binary with `make build` 

Step 2:
- cd to `docker/executable_wrapper`
- edit the Makefile to update `tag` if necessary
- execute `make image`
- execute `make image_push`
