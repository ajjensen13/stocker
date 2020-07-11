ARG GO_VERSION=latest
ARG RUN_TAG=latest

FROM golang:${GO_VERSION} as build
ARG GCFLAGS="-c 1"
ENV CGO_ENABLED=0
ENV GO111MODULE=on
WORKDIR /src/app
RUN go get github.com/go-delve/delve/cmd/dlv
COPY ./go.mod ./go.sum ./
RUN go mod download
COPY . .
RUN go test -json ./...
RUN go build -gcflags "$GCFLAGS" -o /go/bin/app

FROM gcr.io/distroless/base:${RUN_TAG} as run
ENTRYPOINT []
ENV GOTRACEBACK=single
COPY --from=build /go/bin/* /bin/
CMD [ "/bin/app" ]