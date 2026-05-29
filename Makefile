BUILD_DIR = bin

# - go help build
FLAGS = -trimpath
# - go tool compile -h
GCFLAGS = -gcflags=""
# - go tool link -h
LDFLAGS = -ldflags=""

FLAGS += $(GCFLAGS) $(LDFLAGS)

.DEFAULT_GOAL := all
.PHONY: all fmt test lint vuln clean

all: fmt
	@mkdir -p $(BUILD_DIR)/
	@go build -o $(BUILD_DIR)/ $(FLAGS) ./cmd/...

fmt:
	@go fmt ./...

test:
	@go test -v --race ./...

lint:
	@golangci-lint run ./...

vuln:
	@govulncheck ./...

clean:
	@go clean -testcache; rm -rf $(BUILD_DIR)/
