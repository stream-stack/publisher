# Build the manager binary
FROM tangxusc/golang:1.18.1 as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go get github.com/stream-stack/common@latest
RUN go mod download

# Copy the go source
COPY cmd/ cmd/
COPY pkg/ pkg/

ENV GOPROXY="https://goproxy.io,direct"
# Build
RUN CGO_ENABLED=0 go build -a -o publisher cmd/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
#FROM gcr.io/distroless/static:nonroot
FROM ubuntu:latest
WORKDIR /
COPY --from=builder /workspace/publisher .

CMD ["/publisher"]
