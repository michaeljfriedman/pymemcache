# pymemcache/test/test_load_benchmark.py
# pymemcache
# Author: Rushy Panchal
# Description: Benchmark pymemcache hashing clients under load.

import time
import pytest
import hashlib

try:
    import pymemcache.client.hash
    import pymemcache.client.rendezvous_load
    HAS_PYMEMCACHE = True
except Exception:
    HAS_PYMEMCACHE = False

try:
    import numpy.random
    HAS_NUMPYRANDOM = True
except Exception:
    HAS_NUMPYRANDOM=False

_cached_key_order = None
def get_key_order(count, alpha):
    global _cached_key_order
    if _cached_key_order is None:
        _cached_key_order = [str(numpy.random.zipf(alpha)) for i in range(count)]

    return _cached_key_order

def run_zipf_test(name, client, count, alpha):
    client.flush_all()

    key_order = get_key_order(count, alpha)

    for i in range(count):
        value = hashlib.sha256(bytes(i)).digest()
        client.set(str(i), value)

    start = time.time()
    for key in key_order:
        client.get(key)
    duration = time.time() - start
    
    print("{0}: {1}".format(name, duration))

@pytest.mark.benchmark()
@pytest.mark.skipif(not HAS_PYMEMCACHE,
                    reason="requires pymemcache")
@pytest.mark.skipif(not HAS_NUMPYRANDOM,
                    reason="requires numpy.random")
def test_pymemcache_zipf(host, port, size, count, hash_servers, alpha):
    client = pymemcache.client.hash.HashClient(servers=hash_servers)
    run_zipf_test('pymemcache.RendezvousHash', client, count, alpha)

@pytest.mark.benchmark()
@pytest.mark.skipif(not HAS_PYMEMCACHE,
                    reason="requires pymemcache")
@pytest.mark.skipif(not HAS_NUMPYRANDOM,
                    reason="requires numpy.random")
def test_pymemcache_load_zipf(host, port, size, count, hash_servers, alpha):
    print(hash_servers)
    client = pymemcache.client.hash.HashClient(
      servers=hash_servers,
      hasher=pymemcache.client.rendezvous_load.RendezvousLoadHash)
    run_zipf_test('pymemcache.RendezvousLoadHash', client, count, alpha)
