TEST_TARGETS = ./...

test:
	go test ${TEST_TARGETS}

test-coverage:
	go test -coverprofile=coverage.txt -covermode count ${TEST_TARGETS}
