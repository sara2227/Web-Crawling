"""
Word count topology
"""

from streamparse import Grouping, Topology

from bolts.isna_bolt import WordCountBolt
from spouts.isna_spout import WordSpout


class WordCount(Topology):
    word_spout = WordSpout.spec()
    count_bolt = WordCountBolt.spec(inputs=word_spout, par=1)