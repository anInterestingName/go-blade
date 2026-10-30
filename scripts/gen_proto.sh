#!/usr/bin/env bash
#
# 跨平台 Protobuf 桩代码生成脚本 (Linux / macOS Shell)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "==> 进入工程根目录: ${PROJECT_ROOT}"
cd "${PROJECT_ROOT}"

# 1. 尝试追加 GOPATH/bin 至 PATH
if [ -n "${GOPATH:-}" ]; then
    export PATH="${GOPATH}/bin:${PATH}"
elif [ -d "${HOME}/go/bin" ]; then
    export PATH="${HOME}/go/bin:${PATH}"
fi

# 2. 检查依赖工具
if ! command -v protoc &> /dev/null; then
    echo "[错误] 未检测到 protoc 编译器！" >&2
    echo "请先安装 Protocol Buffers 编译器：" >&2
    echo "  - Linux: sudo apt-get install protobuf-compiler" >&2
    echo "  - macOS: brew install protobuf" >&2
    exit 1
fi

if ! command -v protoc-gen-go &> /dev/null; then
    echo "[提示] 未检测到 protoc-gen-go 插件，正在尝试自动安装..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "[提示] 未检测到 protoc-gen-go-grpc 插件，正在尝试自动安装..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

echo "==> 工具链检查完毕:"
protoc --version
echo "protoc-gen-go: $(command -v protoc-gen-go)"
echo "protoc-gen-go-grpc: $(command -v protoc-gen-go-grpc)"

# 3. 递归编译 api/proto 下的所有 proto 文件
find api/proto -name "*.proto" | while read -r proto_file; do
    echo "==> 正在编译 Protobuf 契约: ${proto_file}"
    protoc \
        -I . \
        --go_out=. \
        --go_opt=paths=source_relative \
        --go-grpc_out=. \
        --go-grpc_opt=paths=source_relative \
        "${proto_file}"
done

echo "==> Protobuf 桩代码编译成功！"
