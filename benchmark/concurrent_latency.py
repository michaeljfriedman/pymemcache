# concurrent_latency.py
# Benchmark the memcached server latency across a Zipfian distribution with
# concurrent clients.

import sys
import subprocess
import os.path

BASE_DIR = os.path.dirname(__file__)
BENCHMARK_COMMAND = ['python', os.path.join(BASE_DIR, 'latency.py')]

def main():
  if len(sys.argv) < 2:
    sys.stderr.write('usage: python concurrent_latency.py CONCURRENCY ARGS...\n')
    sys.exit(1)

  number_concurrent = int(sys.argv[1])
  other_args = sys.argv[2:]

  # To achieve true concurrency in Python, it's easiest to just spawn multiple
  # processes. Threads aren't truly concurrent.
  processes = []
  for _ in range(number_concurrent):
    p = subprocess.Popen(
      BENCHMARK_COMMAND + other_args,
      stdout=subprocess.PIPE)
    processes.append(p)

  for p in processes:
    p.wait()
    for line in p.stdout:
      sys.stdout.write(line)

  sys.exit(0)

if __name__ == '__main__':
  main()
