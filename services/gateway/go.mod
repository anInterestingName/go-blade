module github.com/go-blade/services/gateway

go 1.26.0

require (
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/glebarez/sqlite v1.11.0
	github.com/go-blade/api v0.0.0
	github.com/go-blade/pkg v0.0.0
	github.com/go-blade/services/auth v0.0.0
	github.com/go-blade/services/desk v0.0.0
	github.com/go-blade/services/system v0.0.0
	github.com/stretchr/testify v1.9.0
	github.com/zeromicro/go-zero v1.7.6
	google.golang.org/grpc v1.67.3
	gorm.io/gorm v1.31.2
)

replace (
	github.com/go-blade/api => ../../api
	github.com/go-blade/pkg => ../../pkg
	github.com/go-blade/services/auth => ../auth
	github.com/go-blade/services/desk => ../desk
	github.com/go-blade/services/system => ../system
)
