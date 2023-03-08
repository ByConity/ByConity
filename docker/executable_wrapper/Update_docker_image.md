In order to update to update docker image for executable wrapper
Execute wrapper docker image is a wrapper of executable binary for ByConity.

Step 1: build executable binary for ByConity
- From root folder of git repo, rm 
- cd to `docker/builder`
- Build binary with `make build' 

Step 2:
- cd to `docker/executable_wrapper`
- edit the Makefile to update `tag` if necessary
- execute `make build`
- execute `make image_push`
