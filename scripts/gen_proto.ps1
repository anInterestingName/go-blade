<#
.SYNOPSIS
    跨平台 Protobuf 桩代码生成脚本 (PowerShell)
.DESCRIPTION
    检测本地环境中的 protoc、protoc-gen-go 与 protoc-gen-go-grpc，
    并自动编译 api/proto/**/*.proto 到对应目录的 Go 代码桩文件。
#>

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

Write-Host "==> 进入工程根目录: $ProjectRoot"
Set-Location $ProjectRoot

# 1. 尝试将 GOPATH/bin 与用户常见 bin 路径追加至当前 PATH
if ($env:GOPATH) {
    $GoBin = Join-Path $env:GOPATH "bin"
    if (Test-Path $GoBin) {
        $env:Path = "$GoBin;$env:Path"
    }
} else {
    $DefaultGoBin = Join-Path $env:USERPROFILE "go\bin"
    if (Test-Path $DefaultGoBin) {
        $env:Path = "$DefaultGoBin;$env:Path"
    }
}

# 2. 检查依赖工具
$ProtocCmd = Get-Command protoc -ErrorAction SilentlyContinue
$GenGoCmd = Get-Command protoc-gen-go -ErrorAction SilentlyContinue
$GenGrpcCmd = Get-Command protoc-gen-go-grpc -ErrorAction SilentlyContinue

if (-not $ProtocCmd) {
    Write-Error @"
[错误] 未检测到 protoc 编译器！
请先安装 Protocol Buffers 编译器：
  - Windows: winget install Google.Protobuf 或 choco install protoc
  - Linux:   sudo apt install protobuf-compiler
  - macOS:   brew install protobuf
"@
    exit 1
}

if (-not $GenGoCmd) {
    Write-Host "[提示] 未检测到 protoc-gen-go 插件，正在尝试自动安装..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
}

if (-not $GenGrpcCmd) {
    Write-Host "[提示] 未检测到 protoc-gen-go-grpc 插件，正在尝试自动安装..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
}

Write-Host "==> 工具链检查完毕:"
protoc --version
Write-Host "protoc-gen-go: $(Get-Command protoc-gen-go).Source"
Write-Host "protoc-gen-go-grpc: $(Get-Command protoc-gen-go-grpc).Source"

# 3. 编译 api/proto/auth/v1/auth.proto
$ProtoFiles = Get-ChildItem -Path "api/proto" -Filter "*.proto" -Recurse

foreach ($file in $ProtoFiles) {
    $relPath = $file.FullName.Substring($ProjectRoot.Length + 1).Replace("\", "/")
    Write-Host "==> 正在编译 Protobuf 契约: $relPath"
    
    # 执行 protoc 命令
    & protoc `
        -I . `
        --go_out=. `
        --go_opt=paths=source_relative `
        --go-grpc_out=. `
        --go-grpc_opt=paths=source_relative `
        $relPath

    if ($LASTEXITCODE -ne 0) {
        Write-Error "编译失败: $relPath (exit code: $LASTEXITCODE)"
        exit $LASTEXITCODE
    }
}

Write-Host "==> Protobuf 桩代码编译成功！"
