#!/usr/bin/env bash
#
# Runs the peer throughput benchmark matrix and writes one CSV file.
#
#   usage: run.sh <path-to-peer-throughput> <output.csv> [repeats] [seconds]
#
# Each scenario runs in its own process so that the reported peak RSS belongs to that scenario.
# There are three families:
#
#   fast     - large kernel socket buffers and a receiver that drains at full speed. The peer's
#              synchronous send path dominates and _tx_buffer is barely touched.
#   bp       - a small SO_SNDBUF forces outgoing data into _tx_buffer, with the backlog capped at
#              4 MB.
#   bp-deep  - the same, with the backlog capped at 16 MB. Because every partial send memmoves the
#              whole remaining backlog, this is where the transmit buffer's cost shows up.
#   backlog  - a sweep of backlog depths at a fixed payload size, which exposes how throughput
#              scales with the amount of buffered data.

set -u

binary=${1:?usage: run.sh <binary> <output.csv> [repeats] [seconds]}
output=${2:?usage: run.sh <binary> <output.csv> [repeats] [seconds]}
repeats=${3:-3}
seconds=${4:-3}

taskset_prefix=()
if command -v taskset >/dev/null 2>&1 && [ "$(nproc)" -ge 4 ]; then
    taskset_prefix=(taskset -c 2,3)
fi

echo "label,transport,payload,tx_buffer,rx_buffer,sndbuf,rcvbuf,throttle_mbps,elapsed_s,rx_bytes,rx_packets,mb_per_s,packets_per_s,cpu_user_s,cpu_sys_s,cpu_s_per_mb,maxrss_kb,closed_early" > "$output"

run_one()
{
    local label=$1; shift

    for run in $(seq 1 "$repeats"); do
        printf '  %-24s run %d/%d ... ' "$label" "$run" "$repeats" >&2
        if "${taskset_prefix[@]}" "$binary" --csv --transport ws --seconds "$seconds" \
                --label "$label" "$@" >> "$output" 2>/dev/null; then
            tail -n1 "$output" | awk -F, '{printf "%9.1f MB/s\n", $12}' >&2
        else
            tail -n1 "$output" | awk -F, '{printf "%9.1f MB/s (closed early / no data)\n", $12}' >&2
        fi
    done
}

for payload in 64 1k 64k 1M; do
    run_one "fast-${payload}"    --payload "$payload" --inflight 4M
    run_one "bp-${payload}"      --payload "$payload" --sndbuf 64k --rcvbuf 64k --inflight 4M
    run_one "bp-deep-${payload}" --payload "$payload" --sndbuf 64k --rcvbuf 64k --inflight 16M
done

for backlog in 1M 2M 4M 8M 16M; do
    run_one "backlog-${backlog}" --payload 64k --sndbuf 64k --rcvbuf 64k --inflight "$backlog"
done

echo >&2
echo "wrote $output" >&2
