BIN := main

FLAGS = -trimpath
GCFLAGS = -gcflags=""
LDFLAGS = -ldflags=""

FLAGS += $(GCFLAGS) $(LDFLAGS)

.DEFAULT_GOAL := all

.PHONY: all fmt test run lint vuln clean

all: $(BIN)

$(BIN): fmt
	@go build -o $@ $(FLAGS)

fmt:
	@go fmt ./...

test:
	@go test -v ./...

run: all
	@./$(BIN)

lint:
	@golangci-lint run

vuln: lint
	@govulncheck ./...

clean:
	@go clean -testcache; rm -f $(BIN)
