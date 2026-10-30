module github.com/go-blade/services/system

go 1.26.0

require (
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/glebarez/sqlite v1.11.0
	github.com/go-blade/api v0.0.0
	github.com/go-blade/pkg v0.0.0
	github.com/zeromicro/go-zero v1.7.6
	golang.org/x/crypto v0.31.0
	google.golang.org/grpc v1.67.3
	google.golang.org/protobuf v1.36.5
	gorm.io/gorm v1.31.2
)

replace (
	github.com/go-blade/api => ../../api
	github.com/go-blade/pkg => ../../pkg
)
