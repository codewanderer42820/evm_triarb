GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -extldflags=-static" -trimpath -gcflags="all=-l -B" -o ethos .

GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -trimpath -gcflags="all=-l -B" -o ethos .
