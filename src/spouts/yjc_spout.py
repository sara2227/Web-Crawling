from streamparse import Spout
import pandas as pd
import time
import os
import pysolr
from datetime import datetime  # ***
from datetime import timedelta  # ***


class WordSpout(Spout):
    outputs = ["url"]

    def initialize(self, stormconf, context):
        self.in_path = '/home/tookai-1/Documents/HodHod/crawler/streamparse/websites/csv_files/yjc.csv'
        self.url_pd = pd.read_csv(self.in_path,skiprows=[i for i in range(1,1200)])

    def next_tuple(self):
        for index, row in self.url_pd.iterrows():
            self.logger.info("emittedddddddddddddddddddddddddddddddddd ")
            time.sleep(20)
            self.emit([row["url"]])
        self.logger.info("no data for emitt, starting to delete ex product data ... ")



