#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

echo "1. Cleaning old output files..."
rm -f mr-out* mr-[0-9]* my-wc-all correct-wc-all

echo "2. Building wc.so plugin..."
go build -buildmode=plugin ../mrapps/wc.go

echo "3. Running sequential MapReduce for reference output..."
go run mrsequential.go wc.so pg-*.txt
sort mr-out-0 > correct-wc-all
rm -f mr-out*

echo "4. Starting Coordinator in background..."
go run mrcoordinator.go pg-*.txt &
COORD_PID=$!
trap "kill $COORD_PID 2>/dev/null || true" EXIT
sleep 1

echo "5. Running Worker..."
go run mrworker.go wc.so

echo "6. Comparing distributed output (mr-out-*) with sequential output..."
cat mr-out-* | sort > my-wc-all

if cmp -s my-wc-all correct-wc-all; then
    echo "✅ PASS: Distributed output matches sequential output 100%!"
else
    echo "❌ FAIL: Output differs from sequential output!"
    diff -u correct-wc-all my-wc-all | head -n 20
    exit 1
fi

rm -f my-wc-all correct-wc-all
