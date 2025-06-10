TEST_TARGETS = ./iter/... ./util/... ./loghttp/... ./logql/... ./ingester/... ./pattern/... ./helpers/... ./storage/...

test:
	go test ${TEST_TARGETS}

test-coverage:
	go test -coverprofile=coverage.txt -covermode count ${TEST_TARGETS}
