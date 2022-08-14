import os
import json
import uuid
import yaml

import numpy as np
import pandas as pd 

from streamparse import Bolt
from qdrant_client import QdrantClient


import requests
from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


from color_extractor import FromJson, FromFile
from docopt import docopt

from colormath.color_objects import sRGBColor, LabColor
from colormath.color_conversions import convert_color 


class ImageEmbeddingBolt(Bolt):
    outputs = ["url","image_id","product_category", "main_category", "gender"]

    def initialize(self, conf, ctx):

        # reading the config file
        with open("/opt/config/config.yaml") as yamlfile:
            self.config = yaml.load(yamlfile,Loader=yaml.FullLoader)
        
        self.collection_name = self.config['collections']['vector']
        self.batch_size = 1 
        self.parallel = 1

        self.qdrant_con = QdrantClient(host=self.config['qdrant']['hostname'], port=self.config['qdrant']['port'])

        # Vit model address
        self.model = "http://"+ self.config['deepservice']['hostname'] + ":" + self.config['deepservice']['port'] + "/predictions/" + self.config['deepservice']['model_name']

        # initialize the session
        self.image_dir = self.config['path']['image_path']

        self.retry_strategy = Retry(
                    total=50,
                    backoff_factor=1000,
                    status_forcelist=[429, 502, 503, 504 ]
                    )
        self.adapter = HTTPAdapter(max_retries=self.retry_strategy )
        self.adapter.max_retries.respect_retry_after_header = False
        self.session = requests.Session()
        self.session.mount("https://",  self.adapter)
        self.session.mount("http://",  self.adapter)

    
    # create payloads 
    def get_spec(self,tup):
        payloads = [{
        "product_id": tup.values[1],
        "product_category" : tup.values[2],
        "main_category" : tup.values[3],
        "gender" : tup.values[4],
        }]

        image_url = tup.values[0]
        ids = [tup.values[1]]
        return ids,payloads,image_url

    def image_download(self, image_url ):

        img_name = image_url.split("/")[-1]
        data_img = self.session.get(image_url,timeout=50,headers={'x-test2': 'true'}) 
        with open(self.image_dir + img_name, 'wb') as fobj:
            fobj.write(data_img.content) 

        return self.image_dir+img_name

    def remove_image(self, image_path):
        os.remove(image_path)
        return


    def process(self, tup):
        if tup.values[0] != 'unknown_url':
            try:
                
                # get product specs
                ids, payloads, image_url = self.get_spec(tup)
                # Download the image
                image_path = self.image_download(image_url)
                # iamge embedding 
                res = requests.post(self.model, files={'data': open(image_path, 'rb')}).text
                self.remove_image(image_path)
                res = json.loads(res)
                vectors = np.array(res).reshape(1,1024)

                # index at qdrant       
                self.qdrant_con.upload_collection(self.collection_name, vectors , payloads ,ids, self.batch_size, self.parallel)
                self.logger.info("image embedding for production_id: " + str(ids))
                self.logger.info("image embedding for url" + str(image_url))

            except:
                self.logger.info("Image Embedding Error ... " + str(image_url))

        else:
            # kill the topology 
            try:
            
                os.system('storm kill digikala_topology -w 30')
            except :
                self.logger.info("digikala_topology Topolgy Killing Error ... ")

            

