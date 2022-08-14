from operator import imod
import os
from collections import Counter
import json
from streamparse import Bolt
import requests
from bs4 import BeautifulSoup
import time
import pysolr 
from datetime import datetime
from datetime import time as time1
import re
from unidecode import unidecode 
from retry_requests import retry
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from persiantools.jdatetime import JalaliDate
import uuid
from random import randint

class WordCountBolt(Bolt):
    # outputs = ["agency", "url","url_img", "categories", "services", "summary", "title", "content","date_crawl","date","date_persian","news_id"]
    days={"فروردین":1
      ,"اردیبهشت":2
      ,"خرداد":3
      ,"تیر":4
      ,"مرداد":5
      ,"شهریور":6
      ,"مهر":7
      ,"آبان":8
      ,"ابان":8
      ,"آذر":9
      ,"اذر":9
      ,"دی":10
      ,"بهمن":11
      ,"اسفند":12}

    def shamsi_date_creator(self,date_persian):
        tarikh=date_persian.split()
        day=tarikh[0]
        month_str=tarikh[1]
        month = WordCountBolt.days.get(month_str)
        year=tarikh[2]
        date_shamsi = JalaliDate(int(year), int(month), int(day)).isoformat()
        return date_shamsi

    def iso_time_creator(self,date_persian):
        tarikh=date_persian.split()
        date_time_str=tarikh[4].split(':')
        date_time_list=[]
        for i in date_time_str:
            date_time_list.append(int(i))
        if len(date_time_list)==2:
            dt = time1(hour=date_time_list[0], minute=date_time_list[1], microsecond=0)
            return dt.isoformat(timespec='auto')
        elif len(date_time_list)==3:
            dt = time1(hour=date_time_list[0], minute=date_time_list[1], second=date_time_list[2], microsecond=0)
            return dt.isoformat(timespec='auto')

    def initialize(self, conf, ctx):
        self.solr_con = pysolr.Solr("http://192.168.99.155:8983/solr/news_core",always_commit=True, timeout=100)

    def get_news_content(self,url, headers={'User-Agent': 'Mozilla/5.0'}):
        response = requests.get(url=url,headers=headers )
        soup = BeautifulSoup(response.content, 'html.parser')
        img_url = ""
        summary = ""
        content = ""
        categories = ""
        services = ""
        video_url = ""
        if soup.find('p',{'class':'summary introtext'}):
            summary = soup.find('p',{'class':'summary introtext'}).get_text().replace("\n","")
        if soup.find('div',{'class':'item-text'}):
            content = soup.find('div',{'class':'item-text'}).get_text().replace("\n","")
        if soup.find('div',{'class':'gallery'}):
            img_url=[]
            gallery = soup.find('div',{'class':'gallery'}).find_all('figure')
            for img in gallery:
                img_url.append(img.find('img').get("src"))
            if len(img_url)==0:
                if soup.find('figure',{'class':'item-img'}):
                    img_url = soup.find('figure',{'class':'item-img'}).find('img').get('src')
                else:
                    img_url=""
        if soup.find('div',{'class':'path col-xs-12 col-sm-5'}):
            services = soup.find('div',{'class':'path col-xs-12 col-sm-5'}).find('li').get_text().replace('سرویس ','')
        if soup.find('span',{'class':'jwmain'}):
            video_url = soup.find('span',{'class':'jwmain'}).get('src')
        date_persian = soup.find('div',{'class':'col-xs-8 col-sm-6 item-date'}).find('span').get_text()
        date_shamsi = self.shamsi_date_creator(date_persian)
        date_time= str(date_shamsi) +"T" +self.iso_time_creator(date_persian)
        return img_url,summary.strip(), content.strip(), services.strip().replace(' ','‌'), categories.strip().replace(' ','‌'),video_url,date_persian,date_shamsi,date_time

    def process(self, tup):
        self.logger.info("Main url: "+ str(tup.values[0]))

        if tup.values[0] != 'unknown_url':
            main_url = tup.values[0]
            agency = tup.values[1]
            agency_fa = 'مشرق‌نیوز'
            img_url = []
            video_url = ''
            categories = ''
            services = ''
            summary = ''
            title = ''
            content = ''
            date_crawl = ''
            date_time = ''
            date_shamsi = ''
            date_persian = ''
            news_id = ''
            
            retry_strategy = Retry(
                                    total=5,
                                    backoff_factor=0.1,
                                    )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            adapter.max_retries.respect_retry_after_header = False
            http = requests.Session()
            http.mount("https://", adapter)
            http.mount("http://", adapter)

            main_response = http.get(url=main_url, verify=False ,timeout=30,headers={'User-Agent': 'Mozilla/5.0'})
            self.logger.info("Main response: "+ str(main_response))
            main_soup = BeautifulSoup(main_response.content, 'html.parser')
            titles = main_soup.find_all('div',{'class':'desc'})
            self.logger.info("Len news: "+str(len(titles)))
            
            for item in titles:
                try:
                    time.sleep(1)
                    title = item.find('h3').get_text()
                    news_url = "https://www.mashreghnews.ir" + str(item.find('a').get('href'))
                    date_crawl = datetime.now().isoformat(timespec='seconds') + "Z"
                    img_url,summary,content,services,categories,video_url,date_persian,date_shamsi,date_time = self.get_news_content(news_url)
                    news_id = uuid.uuid5(uuid.NAMESPACE_URL, news_url).hex

                    self.logger.info("News url: "+str(news_url))
                    # self.logger.info("title:  "+title+"\nservices:  "+services+"\nnews_url:  "+news_url+"\ncategories:  "
                    # +self.categories+"\ndate_persian:  "+date_persian+"\ndate_shamsi:  "+date_shamsi+'T'+date_time+'Z'
                    # +'\ndate_time:  '+date_time+'Z'+"\ndate_miladi:  "+date_miladi+'T'+'Z'+"\nsummary:  "
                    # +str(summary)+"\ncontent:  "+str(content)+
                    #         "\nimg  "+ img_url+ "\ndate_crawl:  "+date_crawl+ "\nnews_id:  "+news_id+"\n")
                except:         
                    self.logger.info("Get data error." + str(news_url))
                
                try:
                    if date_time == "" or date_time == None or date_time is None:
                        date_time = "0000-00-00T00:00:00Z"
                    if services == "" or services == None or services is None:
                        services = 'نامشخص'
                    if date_persian == "" or date_persian == None or date_persian is None:
                        date_persian = "نا مشخص"
                    if news_id == "" or news_id == None or news_id is None:
                        news_id = agency + "_" + str(random.randint(1, 1004663))
                    if news_url == "" or news_url == None or news_url is None:
                        news_url = "https://www.mashreghnews.ir"


                    self.solr_con.add([
                        {
                            "agency" : agency,
                            "agency_fa" : agency_fa,
                            "url" : news_url,
                            "news_id" : news_id,
                            "url_img" : img_url,
                            "url_video": video_url,
                            "categories" : categories,
                            "services" : services,
                            "summary" : str(summary),
                            "title" : title,
                            "content" : str(content),
                            "date_crawl" : date_crawl,
                            "date" : date_time,
                            "date_persian" : date_persian,
                        }
                    ])
                except:
                    self.logger.info("Index Solr Error." + str(news_url))
                    continue
                

