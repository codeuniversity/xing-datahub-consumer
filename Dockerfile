FROM golang:1.8 as builder
WORKDIR /go/src/github.com/codeuniversity/xing-datahub-consumer
RUN curl https://glide.sh/get | sh
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o consumer .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /go/src/github.com/codeuniversity/xing-datahub-consumer/consumer .
CMD [ "./consumer" ]
EXPOSE 3001
