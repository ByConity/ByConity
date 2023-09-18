# README

This directory has the docker file, Makefile, shell file and configration files which are used create docker wrapper images of ByConity.

## deploy ByConity into physical serves with this docker wrapper

If you want to deploy ByConity into physical serves with this docker wrapper, you can leverage the run.sh file to start ByConity services with docker command. Please check out the instructions from [here](https://byconity.github.io/docs/deployment/docker-wrapper).

## How to use
- Execute following commands in order to bring up corresponding component:
```
./run.sh tso
./run.sh rm
./run.sh server
./run.sh read_worker
./run.sh writer_worker
./run.sh dm
./run.sh cli
```
