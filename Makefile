dep:
	glide install

run:
	go run consumer.go

test:
	go test ./...
