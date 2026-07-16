#!/bin/sh
set -eu

VM_ID=${VM_ID:-118}
DURATION=${DURATION:-24}
BASE=/root/smart14-alpha44-test

qm guest exec "$VM_ID" -- sh -lc "rm -f $BASE/ab-1[1-9]-*.json $BASE/ab-20-*.json" >/dev/null

index=10
for spec in \
    candidate:18143:19143 \
    baseline:18142:19142 \
    baseline:18142:19142 \
    candidate:18143:19143 \
    candidate:18143:19143 \
    baseline:18142:19142 \
    baseline:18142:19142 \
    candidate:18143:19143 \
    candidate:18143:19143 \
    baseline:18142:19142
do
    index=$((index + 1))
    name=${spec%%:*}
    rest=${spec#*:}
    proxy=${rest%%:*}
    api=${rest##*:}
    echo "ROUND-$index-$name"
    qm guest exec "$VM_ID" -- python3 "$BASE/ab_load.py" \
        --name "$name" \
        --api "http://10.254.40.117:$api" \
        --proxy-port "$proxy" \
        --duration "$DURATION" \
        --output "$BASE/ab-$index-$name.json" >/dev/null
done

qm guest exec "$VM_ID" -- python3 "$BASE/ab_aggregate.py" \
    --expected 20 \
    --output "$BASE/ab-summary-20.json"
