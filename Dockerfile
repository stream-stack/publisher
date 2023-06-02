FROM golang:1.20.4 as builder

ENV GOPROXY="https://goproxy.cn,direct"
WORKDIR /workspace
COPY . /workspace/
RUN unset HTTPS_PROXY;unset HTTP_PROXY;unset http_proxy;unset https_proxy;go mod download
RUN unset HTTPS_PROXY;unset HTTP_PROXY;unset http_proxy;unset https_proxy;go get github.com/stream-stack/common
RUN unset HTTPS_PROXY;unset HTTP_PROXY;unset http_proxy;unset https_proxy;go build -o publisher cmd/main.go

FROM ubuntu:latest
WORKDIR /
COPY --from=builder /workspace/publisher .

CMD ["./publisher"]
