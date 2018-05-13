# pymemcache/client/rendezvous_load.py
# Author: Rushy Panchal
# Date: May 7th, 2018
# Description: Rendevouz hashing based on load.

from pymemcache.client.rendezvous import RendezvousHash
from pymemcache.load import LoadManager, rusage_load, cum_req_load
from pymemcache.client.murmur3 import murmur3_32

class RendezvousLoadHash(object):
  '''
  Rendezvous Hashing based on load information.
  '''

  def __init__(self, nodes=None, load_metric=rusage_load):
    self.nodes = set()
    self.load_manager = LoadManager(load_metric=load_metric)

    if not nodes is None:
      for key, server in nodes:
        self.add_node(key, server)

  def add_node(self, key, server):
    self.nodes.add(key)
    self.load_manager.add_server(key, server.server)

  def remove_node(self, key):
    self.nodes.remove(key)
    self.load_manager.remove_server(key)

  def get_node(self, key):
    high_score = -1
    winner_by_score = None
    winner_overall = None

    # server_loads is instantaneous load whereas load_statistics is a moving
    # set of statistics over the load.
    inst_server_loads = self.load_manager.load()
    load_statistics = self.load_manager.load_statistics()

    # Pick highest-weight server that isn't loaded. Otherwise default to
    # server with highest score
    for node in self.nodes:
        score = murmur3_32("%s-%s" % (node, key))
        stats = load_statistics[node]

        load_threshold = stats['average'] + 2*stats['stddev']

        if score > high_score:
          (high_score, winner_by_score) = (score, node)
          if inst_server_loads[node] < load_threshold:
              winner_overall = node

    if not winner_overall:
      winner_overall = winner_by_score

    return winner_overall
