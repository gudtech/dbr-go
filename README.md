# dbr-go

### Running Tests

To build and test:

    mkdir -p $GOPATH/src/github.com/gudtech/
    git clone https://github.com/gudtech/dbr-go $GOPATH/src/github.com/gudtech/dbr-go
    cd $GOPATH/src/github.com/gudtech/dbr-go
    dep ensure -vendor-only
    go test -v ./...
