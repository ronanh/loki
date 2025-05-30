SHELL = /usr/bin/env bash

test:
	go test github.com/ronanh/loki/logql/log github.com/ronanh/loki/pattern github.com/ronanh/loki/logql
