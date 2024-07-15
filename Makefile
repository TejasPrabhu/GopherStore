build:
	@go build -o bin/GopherStore

run: build
	@./bin/GopherStore

test:
	@go test ./... -v