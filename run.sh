GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -extldflags=-static" -trimpath -gcflags="all=-l -B" -o evm_linux .

GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -trimpath -gcflags="all=-l -B" -o evm_darwin .
