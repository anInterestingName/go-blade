# =============================================================================
# go-blade 云原生多阶段构建 Dockerfile
# 用法示例：
#   docker build --build-arg SERVICE_NAME=gateway -t go-blade/service-gateway:latest .
#   docker build --build-arg SERVICE_NAME=auth    -t go-blade/service-auth:latest .
#   docker build --build-arg SERVICE_NAME=system  -t go-blade/service-system:latest .
#   docker build --build-arg SERVICE_NAME=desk    -t go-blade/service-desk:latest .
# =============================================================================

FROM golang:1.26-alpine AS builder

ARG SERVICE_NAME=gateway

WORKDIR /workspace

# 复制 Go Workspace 与各模块源码
COPY go.work ./
COPY api ./api
COPY pkg ./pkg
COPY services ./services

# 编译指定微服务的静态二进制
WORKDIR /workspace/services/${SERVICE_NAME}
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/app ./cmd/api/main.go

# =============================================================================
# 阶段 2：极简安全运行镜像
# =============================================================================
FROM alpine:3.20

ARG SERVICE_NAME=gateway

RUN apk --no-cache add ca-certificates tzdata && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

WORKDIR /app

COPY --from=builder /out/app /app/server
COPY --from=builder /workspace/services/${SERVICE_NAME}/etc /app/etc

EXPOSE 8000 8081 8082 8083 9001 9002 9003

ENTRYPOINT ["/app/server"]
CMD ["-f", "/app/etc/gateway.yaml"]
