#!/bin/bash
# Run concurrency latency test across a range of concurrency values.

start=$1
end=$2
step=$3
client_t=$6

if [ -z ${client_t+x} ]; then
  echo "See usage..."
  exit 1
fi

mkdir -p "data/$client_t/"

for ((c=$start;c<=$end;c+=$step)); do
  python benchmark/concurrent_latency.py $c "${@:4}" > "data/$client_t/benchmark_concurrency_$c"
done
