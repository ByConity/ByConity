#make image TAG=2.0.3 DOCKER_TAG=2.0.3
TAG := latest 
DOCKER_TAG := $(TAG)
image:
	$(info Build with tag is $(TAG) docker tag $(DOCKER_TAG))
	@mkdir -p artifacts 
	@rm -rf artifacts/*
	@if [ $(TAG) = "latest" ]; then \
        echo "fetch clickhouse-test and test cases from local repo"; \
        cp ../../../tests/clickhouse-test ./artifacts/; \
        cp ../stateless/process_functional_tests_result.py ./artifacts/; \
        cp -r ../../../tests/queries ./artifacts/queries; \
    else \
        echo "fetch clickhouse-test and test cases from ByConity tag $(TAG)"; \
        test -d ByConity || git clone git@github.com:ByConity/ByConity.git --branch master; \
        git -C ./ByConity checkout master; \
        git -C ./ByConity pull origin master; \
        git -C ./ByConity checkout $(TAG); \
        cp ./ByConity/tests/clickhouse-test ./artifacts/; \
        cp ./ByConity/docker/test/stateless/process_functional_tests_result.py ./artifacts/; \
        cp -r ./ByConity/tests/queries ./artifacts/queries; \
    fi
	$(info Build docker image byconity/byconity-test with byconity/byconity:$(DOCKER_TAG))
	docker build --network=host -t byconity/byconity-test . --build-arg TAG=$(DOCKER_TAG)
image_push:
	docker push byconity/byconity-test:$(DOCKER_TAG)

test:
	docker run --network=host --rm -it byconity/byconity-test:$(DOCKER_TAG) /bin/bash

