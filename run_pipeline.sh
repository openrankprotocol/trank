#!/bin/bash
set -e

cd "$(dirname "${BASH_SOURCE[0]}")"

CHANNEL_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --channel) CHANNEL_MODE=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ "$CHANNEL_MODE" = true ]; then
    TRUST_SCRIPT="generate_channel_trust.py"
    JSON_SCRIPT="generate_channel_json.py"
else
    TRUST_SCRIPT="generate_trust.py"
    JSON_SCRIPT="generate_json.py"
fi

# Step 1: Generate trust
python3 "$TRUST_SCRIPT"

# Step 2: Run OpenRank
for trust_file in ./trust/*.csv; do
    channel_id=$(basename "$trust_file" .csv)
    RUST_LOG=info openrank compute-local-et \
        "$trust_file" \
        "./seed/${channel_id}.csv" \
        --out-path="scores/${channel_id}.csv" \
        --alpha=0.25 \
        --delta=0.000001
done

# Step 3: Process scores
python3 process_scores.py

# Step 4: Generate JSON
python3 "$JSON_SCRIPT"
