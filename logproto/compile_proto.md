

```
 protoc -I=$GOPATH/src:.:./vendor \
   --gogoslick_out=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,plugins=grpc,paths=source_relative:./ \
   ./pkg/logproto/logproto.proto
 ```