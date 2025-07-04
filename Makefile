TEST_TARGETS = ./...
LINT_PKGS = ./iter ./util

test:
	go test ${TEST_TARGETS}

lint:
	golangci-lint run ${LINT_PKGS}

fmt:
	golangci-lint fmt ${LINT_PKGS}

ci-check: fmt lint
