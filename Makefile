.PHONY: gen test

gen:
	protoc --go_out=. --go-grpc_out=. service.proto

test:
	go test -v -race

