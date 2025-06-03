SHELL = /usr/bin/env bash

test:
	go test ./iter/... ./util/... ./loghttp/... ./logql/... ./ingester/... ./chunkenc/... ./pattern/... ./helpers/...
