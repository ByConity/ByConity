image:
	cp ./../../contrib/foundationdb/lib/libfdb_c.so ../../build/programs/
	cp ./app/run.sh ../../build/programs/
	docker build --network=host -t byconity/byconity-server ../../build/programs -f Dockerfile
image_pull:
	docker pull byconity/byconity-server:v0.2
image_push:
	docker tag byconity/byconity-server:latest byconity/byconity-server:v0.2
	docker push byconity/byconity-server:v0.2
run:
	docker run -it --network host --name dao_tso byconity/byconity-server 
debug_image:
	docker exec -it byconity-tso /bin/bash
prepare_config:
	./config/prepare_config.py
