tag?=v0.2.1
image:
	test -f ../../build/programs/foundationdb-clients_7.1.27-1_amd64.deb || curl -L -o ../../build/programs/foundationdb-clients_7.1.27-1_amd64.deb https://github.com/apple/foundationdb/releases/download/7.1.27/foundationdb-clients_7.1.27-1_amd64.deb
	cp ./app/run.sh ../../build/programs/
	docker build --network=host -t byconity/byconity-server ../../build/programs -f Dockerfile
image_pull:
	docker pull byconity/byconity-server:stable
image_push:
	docker tag byconity/byconity-server:latest byconity/byconity-server:$(tag)
	docker tag byconity/byconity-server:latest byconity/byconity-server:stable
	docker push byconity/byconity-server:$(tag)
	docker push byconity/byconity-server:stable
run_test:
	docker run --rm byconity/byconity-server:$(tag) client --help
debug_image:
	docker exec -it byconity-tso /bin/bash
