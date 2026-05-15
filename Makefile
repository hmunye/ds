BIN := main

FLAGS = -trimpath
GCFLAGS = -gcflags=""
LDFLAGS = -ldflags=""

FLAGS += $(GCFLAGS) $(LDFLAGS)

.DEFAULT_GOAL := all

.PHONY: all fmt test run vet clean

all: $(BIN)

$(BIN): fmt
	@go build -o $@ $(FLAGS)

fmt:
	@go fmt ./...

test:
	@go test ./...

run: all
	@./$(BIN)

vet:
	@golangci-lint run
	@govulncheck ./...

clean:
	@go clean -testcache; rm -f $(BIN)
