go build -ldflags="-s -w" -gcflags="-B" -trimpath -o arb main.go
strip -u -r arb

go test -bench . -benchmem -v -benchtime=1s -count=1
