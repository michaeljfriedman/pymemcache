# pymemcache/client/rendezvous_load.py
# Author: Rushy Panchal
# Date: May 7th, 2018
# Description: Rendevouz hashing based on load.

from pymemcache.client.rendezvous import RendezvousHash
from pymemcache.load import LoadManager
from pymemcache.client.murmur3 import murmur3_32

class RendezvousLoadHash(object):
  '''
  Rendezvous Hashing based on load information.
  '''

  # Servers with instantaneous load >= this value are considered "loaded"
  load_threshold = 100

  def __init__(self, nodes=None):
    self.nodes = set()
    self.load_manager = LoadManager()
    if not nodes is None:
      for key, server in nodes:
        self.add_node(key, server)

  def add_node(self, key, server):
    self.nodes.add(key)
    self.load_manager.add_server(key, server)

  def remove_node(self, key):
    self.nodes.remove(key)
    self.load_manager.remove_server(key)

  def get_node(self, key):
    high_score = -1
    winner_by_score = None
    winner_overall = None

    server_loads = self.load_manager.load()

    # Pick highest-weight server that isn't loaded. Otherwise default to
    # server with highest score
    for node in self.nodes:
        score = murmur3_32("%s-%s" % (node, key))

        if score > high_score:
          (high_score, winner_by_score) = (score, node)
          if server_loads[node] < RendezvousLoadHash.load_threshold:
              winner_overall = node

    if not winner_overall:
      winner_overall = winner_by_score

    return winner_overall
