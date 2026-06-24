module github.com/vinirmbrozz/protobuf-contracts/sdk/go

go 1.23

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20260415201107-50325440f8f2.1
	github.com/vinirmbrozz/protobuf-contracts/gen/go v0.0.0
	google.golang.org/protobuf v1.36.11
)

replace github.com/vinirmbrozz/protobuf-contracts/gen/go => ../../gen/go
