pull:
	docker pull byconity/binary-builder

image:
	test -f nfpm.deb || curl -L -o nfpm.deb https://github.com/goreleaser/nfpm/releases/download/v2.20.0/nfpm_amd64.deb
	docker build -t byconity/binary-builder .
	docker tag byconity/binary-builder byconity/binary-builder:0.1

image_push:
	docker push byconity/binary-builder
