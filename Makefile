.DEFAULT_GOAL := test

clean:
	go clean

fmt:
	go fmt ./...

.PHONY: test
test:
	go test ./...

tag:
	git tag v$(VERSION)
	git push origin v$(VERSION)
