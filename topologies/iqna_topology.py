"""
Word count topology
"""

from streamparse import Grouping, Topology

from bolts.iqna_bolt import WordCountBolt
from spouts.iqna_spout import WordSpout


class WordCount(Topology):
    word_spout = WordSpout.spec()
    count_bolt = WordCountBolt.spec(inputs=word_spout, par=1)
