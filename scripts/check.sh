#!/usr/bin/env bash
set -euo pipefail

TARGET="all"
RACE=0
COVER=0
FUZZ=0
FUZZ_TIME="2s"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--target)
            TARGET="$2"
            shift 2
            ;;
        -r|--race)
            RACE=1
            shift
            ;;
        -c|--cover)
            COVER=1
            shift
            ;;
        -f|--fuzz)
            FUZZ=1
            shift
            ;;
        --fuzz-time)
            FUZZ_TIME="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ./scripts/check.sh [--target all|pkg|auth|gateway|system|desk] [--race] [--cover] [--fuzz] [--fuzz-time 2s]"
            exit 1
            ;;
    esac
done

echo "==> Checking gofmt..."
unformatted=$(gofmt -l pkg api services/auth services/desk services/gateway services/system)
if [ -n "$unformatted" ]; then
    echo "The following files are not formatted with gofmt:"
    echo "$unformatted"
    exit 1
fi
echo "gofmt passed."

TEST_FLAGS=("-v")
if [[ $RACE -eq 1 ]]; then
    if command -v gcc >/dev/null 2>&1; then
        export CGO_ENABLED=1
        TEST_FLAGS+=("-race")
        echo "==> Race detector enabled."
    else
        echo "[WARN] --race requested but gcc not found; falling back to standard tests."
    fi
fi
if [[ $COVER -eq 1 ]]; then
    TEST_FLAGS+=("-cover")
fi

run_module() {
    local name="$1"
    local path="$2"
    local run_test="$3"

    if [[ "$TARGET" != "all" && "$TARGET" != "$name" && "$TARGET" != "$path" ]]; then
        return
    fi
    if [[ ! -d "$path" ]]; then
        return
    fi

    echo "==> Running go vet in $path..."
    (cd "$path" && go vet ./...)
    echo "go vet in $path passed."

    if [[ "$run_test" == "1" ]]; then
        echo "==> Running go test in $path..."
        local flags=("${TEST_FLAGS[@]}")
        if [[ $COVER -eq 1 && "$name" != "pkg" ]]; then
            flags+=("-coverpkg=./internal/...")
        fi
        (cd "$path" && go test "${flags[@]}" ./...)
        echo "go test in $path passed."
    fi
}

run_module "api" "api" "0"
run_module "pkg" "pkg" "1"
run_module "auth" "services/auth" "1"
run_module "gateway" "services/gateway" "1"
run_module "system" "services/system" "1"
run_module "desk" "services/desk" "1"

if [[ $FUZZ -eq 1 && ( "$TARGET" == "all" || "$TARGET" == "pkg" ) ]]; then
    echo "==> Running Go native Fuzz tests in pkg (fuzztime=$FUZZ_TIME)..."
    (cd pkg && go test ./auditlog -run="^$" -fuzz="^FuzzMaskJSON$" -fuzztime="$FUZZ_TIME")
    (cd pkg && go test ./job -run="^$" -fuzz="^FuzzParseCron$" -fuzztime="$FUZZ_TIME")
    (cd pkg && go test ./oss -run="^$" -fuzz="^FuzzBuildTenantObjectKey_NoPathTraversal$" -fuzztime="$FUZZ_TIME")
    echo "All Fuzz tests passed."
fi

echo "All checks passed successfully!"
