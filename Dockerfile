# build stage
FROM golang:alpine as build-env

ARG src_name="workflow-engine"

WORKDIR ${GOPATH}/src/github.com/youritcity/${src_name}
ADD . .
RUN cd server; CGO_ENABLED=0 go build -o /${src_name}

# run stage
FROM alpine

RUN apk --no-cache add ca-certificates
WORKDIR /app

COPY --from=build-env /${src_name} .

CMD [ "./workflow-engine" ]
