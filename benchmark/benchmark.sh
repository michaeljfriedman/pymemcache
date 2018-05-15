#!/bin/bash

set -e

servers="$MEMCACHED_SERVERS"
concurrency="${CONCURRENCY:-2500}"
count="${COUNT:-10000}"
alpha="${ALPHA:-1.135}"
num_keys="${NUMBER_KEYS:-1000000}"
concurrency_range_start="${CONCURRENCY_RANGE_START:-100}"
concurrency_range_end="${CONCURRENCY_RANGE_END:-2500}"
concurrency_range_step="${CONCURRENCY_RANGE_STEP:-100}"
concurrency_range_count="${CONCURRENCY_RANGE_COUNT:-1000}"
client_types=(
  'load'
  'load-rusage'
  'hash')

output_dir="$1"

function print() {
  echo "[$(date)]" "$@"
  }

function run_command() {
  msg="$1"
  cmd="${@:2}"
  print "- Running Task: $msg"
  echo "$cmd"
  eval "$cmd"
  print "+ Finished Task: $msg"
  }

if [ $# -ne 1 ]; then
  print "Usage: benchmark.sh OUTPUT_FOLDER"
  exit 1
fi

if [ -z "$servers" ]; then
  print "Please set the MEMCACHED_SERVERS environment variable to a list of servers."
  exit 1
fi

print "--- Starting Benchmark ---"

print "@ concurrency=$concurrency; count=$count; alpha=$alpha; number_keys=$num_keys; concurrency_range_start=$concurrency_range_start; concurrency_range_end=$concurrency_range_end; concurrency_range_step=$concurrency_range_step; concurrency_range_count=$concurrency_range_count; output_dir=$output_dir"

print "@ Servers to Benchmark:"
for server in $servers; do
  echo "$server"
done

if [ -d "$output_dir" ]; then
  print "Output directory $output_dir already exists. Exiting to avoid overwriting data..."
  exit 1
fi

# Actual benchmarks start now.
# Confirm user.
read -p "Start the benchmark? " -n 1 -r
echo
if ! [[ $REPLY =~ ^[Yy]$ ]]; then
  echo "Cancelling..."
  exit 2
fi

run_command "Make data directory" mkdir -p "$output_dir"

run_command "Initialize Cache" python benchmark/initialize.py "$num_keys $alpha $servers > $output_dir/keys"

for client_t in "${client_types[@]}"; do
  run_command "Make $client_t data directory" mkdir -p "$output_dir/$client_t"

  run_command "Benchmark 1 on $client_t" python benchmark/latency.py "$concurrency $count $alpha $output_dir/keys 0 $client_t $servers > $output_dir/$client_t/overall_$concurrency"
  sleep 5
  
  for ((c=$concurrency_range_start; c<=$concurrency_range_end; c+=$concurrency_range_step)); do
    run_command "Benchmark 2.$c on $client_t" python benchmark/latency.py "$c $concurrency_range_count $alpha $output_dir/keys 0 $client_t $servers > $output_dir/$client_t/range_$c"
    sleep 5
  done

  run_command "Aggregate range statistics for $client_t" python benchmark/aggregate_stats.py "$output_dir/$client_t/range_ $output_dir/$client_t/range_* > $output_dir/$client_t/aggregate_range"
done

print "> Generated Data:"
tree "$output_dir"

echo "--- Benchmark Finished ---"
