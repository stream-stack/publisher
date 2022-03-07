image-build:
	docker build -f Dockerfile -t ccr.ccs.tencentyun.com/stream/stream:publisher-v1 .
image-push:
	docker push ccr.ccs.tencentyun.com/stream/stream:publisher-v1
all: image-build image-push
local:
	cd cmd && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o publisher main.go &&cd .. && docker build -f Dockerfile-local -t ccr.ccs.tencentyun.com/stream/stream:publisher-v1 .