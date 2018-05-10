# benchmark.py
# Benchmark the memcached server across a Zipfian distribution.

from pymemcache.client.hash import HashClient
from pymemcache.client.rendezvous_load import RendezvousLoadHash

import time
import sys
import numpy.random
import hashlib

def main():
  if len(sys.argv) < 3:
    sys.stderr.write('usage: python benchmark.py COUNT ALPHA CLIENT HOST...\n')
    sys.exit(1)

  count = int(sys.argv[1])
  alpha = int(sys.argv[2])
  client_t = sys.argv[3]
  hosts = sys.argv[4:]

  parsed_hosts = []
  for h in hosts:
    host, port = h.split(':')
    parsed_hosts.append((host, int(port)))

  if client_t == 'hash':
    client = HashClient(servers=parsed_hosts)
  elif client_t == 'load':
    client = HashClient(servers=parsed_hosts, hasher=RendezvousLoadHash)
  else:
    sys.stderr.write('client_t must be one of: hash, load\n')
    sys.exit(1)

  client.flush_all()
  key_set = set()

  for i in range(count):
    key = str(numpy.random.zipf(alpha))
    if not key in key_set:
      key_set.add(key)
      value = hashlib.sha256(bytes(key)).digest()
      client.set(key, value)

    start_time = time.time()
    client.get(key)
    elapsed = time.time() - start_time
    
    print(elapsed)

  sys.exit(0)

if __name__ == '__main__':
  main()
