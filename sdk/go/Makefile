.PHONY: test tidy

tidy:
	go mod tidy

test: tidy
	go test -v -race ./...
