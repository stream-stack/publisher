# Build the manager binary
FROM tangxusc/golang:1.18.1 as builder

ENV GOPROXY="https://goproxy.io,direct"
WORKDIR /workspace
COPY . /workspace
RUN go mod download

RUN CGO_ENABLED=0 go build -a -o publisher cmd/main.go

FROM ubuntu:latest
WORKDIR /
COPY --from=builder /workspace/publisher .

CMD ["/publisher"]
