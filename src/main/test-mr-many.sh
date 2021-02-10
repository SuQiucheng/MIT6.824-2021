#!/bin/sh

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
runs=$1
chmod +x test-mr.sh
for i in $(seq 1 $runs); do
    timeout -k 2s 900s ./test-mr.sh
done
