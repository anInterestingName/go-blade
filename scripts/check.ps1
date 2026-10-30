param(
    [string]$Target = "all",
    [switch]$Race,
    [switch]$Cover,
    [switch]$Fuzz,
    [string]$FuzzTime = "2s"
)

$ErrorActionPreference = "Stop"

$AllModules = @(
    @{ Name = "api";     Path = "api";              RunTests = $false },
    @{ Name = "pkg";     Path = "pkg";              RunTests = $true  },
    @{ Name = "auth";    Path = "services/auth";    RunTests = $true  },
    @{ Name = "gateway"; Path = "services/gateway"; RunTests = $true  },
    @{ Name = "system";  Path = "services/system";  RunTests = $true  },
    @{ Name = "desk";    Path = "services/desk";    RunTests = $true  }
)

# 过滤目标模块
$SelectedModules = @()
if ($Target -eq "all" -or [string]::IsNullOrWhiteSpace($Target)) {
    $SelectedModules = $AllModules
} else {
    foreach ($m in $AllModules) {
        if ($m.Name -eq $Target -or $m.Path -eq $Target) {
            $SelectedModules += $m
        }
    }
    if ($SelectedModules.Count -eq 0) {
        Write-Host "Unknown target '$Target'. Available targets: all, api, pkg, auth, gateway, system, desk" -ForegroundColor Red
        exit 1
    }
}

# 1. gofmt 格式检查
Write-Host "==> Checking gofmt..." -ForegroundColor Cyan
$gofmtPaths = $SelectedModules | ForEach-Object { $_.Path } | Where-Object { Test-Path $_ }
$gofmtFiles = gofmt -l @gofmtPaths
if ($gofmtFiles) {
    Write-Host "The following files are not formatted with gofmt:" -ForegroundColor Red
    $gofmtFiles
    exit 1
}
Write-Host "gofmt passed." -ForegroundColor Green

# 2. Protobuf 契约生成物同步检查
if (Test-Path "api/proto") {
    Write-Host "`n==> Checking Protobuf contract synchronization..." -ForegroundColor Cyan
    $protoFiles = Get-ChildItem -Path "api/proto" -Filter "*.proto" -Recurse
    foreach ($proto in $protoFiles) {
        $dir = $proto.DirectoryName
        $base = [System.IO.Path]::GetFileNameWithoutExtension($proto.Name)
        $pbFile = Join-Path $dir "$base.pb.go"
        if (-not (Test-Path $pbFile)) {
            Write-Host "Missing generated file for $($proto.FullName): $pbFile" -ForegroundColor Red
            exit 1
        }
    }
    Write-Host "Protobuf contracts verified." -ForegroundColor Green
}

# 3. 检测 -Race 所需的 CGO / GCC 编译器环境
$UseRace = $false
if ($Race) {
    $gccCmd = Get-Command gcc -ErrorAction SilentlyContinue
    if ($null -ne $gccCmd) {
        $env:CGO_ENABLED = "1"
        $UseRace = $true
        Write-Host "`n==> Race detector enabled (CGO_ENABLED=1, gcc found)." -ForegroundColor Cyan
    } else {
        Write-Host "`n[WARN] -Race requested but 'gcc' compiler not found in PATH; falling back to standard concurrency tests." -ForegroundColor Yellow
    }
}

$SummaryRows = @()

# 4. 逐模块执行 go vet 与 go test
foreach ($mod in $SelectedModules) {
    $modPath = $mod.Path
    $modName = $mod.Name
    if (-not (Test-Path $modPath)) {
        continue
    }

    Write-Host "`n==> Running go vet in $modPath..." -ForegroundColor Cyan
    Push-Location $modPath
    try {
        go vet ./...
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        Write-Host "go vet in $modPath passed." -ForegroundColor Green

        if ($mod.RunTests) {
            Write-Host "`n==> Running go test in $modPath..." -ForegroundColor Cyan
            $sw = [System.Diagnostics.Stopwatch]::StartNew()

            $testArgs = @("test", "-v")
            if ($UseRace) {
                $testArgs += "-race"
            }
            if ($Cover) {
                $testArgs += "-cover"
                if ($modName -ne "pkg") {
                    $testArgs += "-coverpkg=./internal/..."
                }
            }
            $testArgs += "./..."

            & go @testArgs
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            $sw.Stop()
            Write-Host "go test in $modPath passed." -ForegroundColor Green

            $SummaryRows += [PSCustomObject]@{
                Module   = $modName
                Path     = $modPath
                Vet      = "PASS"
                Test     = "PASS"
                Duration = "$([math]::Round($sw.Elapsed.TotalSeconds, 2))s"
            }
        } else {
            $SummaryRows += [PSCustomObject]@{
                Module   = $modName
                Path     = $modPath
                Vet      = "PASS"
                Test     = "N/A (Contract)"
                Duration = "-"
            }
        }
    } finally {
        Pop-Location
    }
}

# 5. 可选执行原生 Fuzz 模糊测试
if ($Fuzz -and ($Target -eq "all" -or $Target -eq "pkg")) {
    Write-Host "`n==> Running Go native Fuzz tests in pkg (FuzzTime=$FuzzTime)..." -ForegroundColor Cyan
    Push-Location pkg
    try {
        $fuzzTargets = @(
            @{ Pkg = "./auditlog"; Func = "FuzzMaskJSON" },
            @{ Pkg = "./job";      Func = "FuzzParseCron" },
            @{ Pkg = "./oss";      Func = "FuzzBuildTenantObjectKey_NoPathTraversal" }
        )
        foreach ($ft in $fuzzTargets) {
            Write-Host "  -> Fuzzing $($ft.Pkg) :: $($ft.Func) ($FuzzTime)..." -ForegroundColor Cyan
            $fuzzArgs = @("test", $ft.Pkg, "-run=^$", "-fuzz=^$($ft.Func)$", "-fuzztime=$FuzzTime")
            & go @fuzzArgs
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }
        Write-Host "All Fuzz tests passed." -ForegroundColor Green
    } finally {
        Pop-Location
    }
}

# 6. 输出本地自动化测试汇总报表
Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "             go-blade Local Test Suite Summary              " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
$SummaryRows | Format-Table -AutoSize
Write-Host "All checks passed successfully!" -ForegroundColor Green
