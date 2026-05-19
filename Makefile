BIN := node
BUILD_DIR := bin

FLAGS = -trimpath
GCFLAGS = -gcflags=""
LDFLAGS = -ldflags=""

FLAGS += $(GCFLAGS) $(LDFLAGS)

.DEFAULT_GOAL := all

.PHONY: all fmt test run lint vuln clean

all: $(BIN)

$(BIN): fmt
	@go build -o $(BUILD_DIR)/$@ $(FLAGS) .

fmt:
	@go fmt ./...

test:
	@go test -v ./...

run: all
	@./$(BUILD_DIR)/$(BIN)

lint:
	@golangci-lint run ./...

vuln: lint
	@govulncheck ./...

clean:
	@go clean -testcache; rm -rf $(BUILD_DIR)
