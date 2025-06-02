SHELL = /usr/bin/env bash

test:
	go test github.com/ronanh/loki/pkg/logql/log github.com/ronanh/loki/pkg/pattern github.com/ronanh/loki/pkg/logql
