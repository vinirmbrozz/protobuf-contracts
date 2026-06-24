module github.com/vinirmbrozz/protobuf-contracts/interop/go

go 1.23

require github.com/vinirmbrozz/protobuf-contracts/sdk/go v0.0.0

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20260415201107-50325440f8f2.1 // indirect
	github.com/vinirmbrozz/protobuf-contracts/gen/go v0.0.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/vinirmbrozz/protobuf-contracts/sdk/go => ../../sdk/go

replace github.com/vinirmbrozz/protobuf-contracts/gen/go => ../../gen/go
