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
    days={"فروردین":1,"اردیبهشت":2,"خرداد":3,"تیر":4,"مرداد":5,"شهریور":6,"مهر":7,"آبان":8,"ابان":8,"آذر":9,"اذر":9,"دی":10,"بهمن":11,"اسفند":12}


    def initialize(self, conf, ctx):
        self.solr_con = pysolr.Solr("http://192.168.99.155:8983/solr/news_core",always_commit=True, timeout=100)

    def get_news_content(self,url, headers={'User-Agent': 'Mozilla/5.0'}):
        response = requests.get(url=url,headers=headers )
        soup = BeautifulSoup(response.content, 'html.parser')
        img_url=""
        video_url=""
        summary=""
        content=""
        services=""
        categories=""
        if soup.find('source'):
            video_url = soup.find('source').get('src')
            summary = soup.find('p',{'class':'summary introtext'}).get_text().replace("\n","")
            content = soup.find('p',{'class':'summary introtext'}).get_text().replace("\n","")
            services = soup.find_all('li',{'class':'breadcrumb-item'})[1].find('a').get_text().replace('سرویس ','')
            if len(soup.find('ol',{'class':'breadcrumb'}).find_all('li'))==3:
                categories = soup.find('li',{'class':'breadcrumb-item active'}).find('a').get_text()
            else:
                categories= ""
        else:
            if soup.find('section',{'class':'box content story photoGall'}):
                img_url=[]
                gallery = soup.find('section',{'class':'box content story photoGall'}).find_all('li',{'class':''})
                for img in gallery:
                    img_url.append(img.find('figure').find('a').get("href"))
                    summary = soup.find('p',{'class':'introtext'}).get_text().replace("\n","")
                    content = soup.find('p',{'class':'introtext'}).get_text().replace("\n","")
                    services = soup.find('ol',{'class':'breadcrumb'}).find_all('li')[0].get_text().replace('سرویس ','')
                    if len(soup.find('ol',{'class':'breadcrumb'}).find_all('li'))==2:
                        categories = soup.find('ol',{'class':'breadcrumb'}).find_all('li')[1].get_text()
                    else:
                        categories = ""
                if len(img_url)==0:
                    if soup.find('div',{'class':'item-body'}).find('p').find('img'):
                        img_url = soup.find('div',{'class':'item-body'}).find('p').find('img').get('src')
                        summary = soup.find('p',{'class':'introtext'}).get_text().replace("\n","")
                        content = soup.find('p',{'class':'introtext'}).get_text().replace("\n","")
                        services = soup.find('ol',{'class':'breadcrumb'}).find_all('li')[0].get_text().replace('سرویس ','')
                        if len(soup.find('ol',{'class':'breadcrumb'}).find_all('li'))==2:
                            categories = soup.find('ol',{'class':'breadcrumb'}).find_all('li')[1].find('a').get_text()
                        else:
                            categories = ""
                    elif soup.find('li',{'class':'large-item'}).find('figure').find('a'):
                        img_url = soup.find('li',{'class':'large-item'}).find('figure').find('a').get('href')
                        summary = soup.find('p',{'class':'introtext'}).get_text().replace("\n","")
                        content = soup.find('p',{'class':'introtext'}).get_text().replace("\n","")
                        services = soup.find('ol',{'class':'breadcrumb'}).find_all('li')[0].get_text().replace('سرویس ','')
                        if len(soup.find('ol',{'class':'breadcrumb'}).find_all('li'))==2:
                            categories = soup.find('ol',{'class':'breadcrumb'}).find_all('li')[1].find('a').get_text()
                        else:
                            categories = ""
            elif soup.find('figure',{'class':'item-img'}):
                img_url = soup.find('figure',{'class':'item-img'}).find('img').get('src')
                video_url=""
                summary = soup.find('p',{'class':'summary introtext'}).get_text().replace("\n","")
                content = soup.find('div',{'class':'item-text'}).get_text().replace("\n","")
                services = soup.find_all('li',{'class':'breadcrumb-item'})[1].find('a').get_text().replace('سرویس ','')
                if len(soup.find('ol',{'class':'breadcrumb'}).find_all('li'))==3:
                    categories = soup.find_all('li',{'class':'breadcrumb-item'})[2].find('a').get_text()
                else:
                    categories = ""
            else:
                summary = soup.find('p',{'class':'summary introtext'}).get_text().replace("\n","")
                content = soup.find('div',{'class':'item-text'}).get_text().replace("\n","")
                services = soup.find_all('li',{'class':'breadcrumb-item'})[1].find('a').get_text().replace('سرویس ','')
                if len(soup.find('ol',{'class':'breadcrumb'}).find_all('li'))==3:
                    categories = soup.find_all('li',{'class':'breadcrumb-item'})[2].find('a').get_text()
                else:
                    categories = ""

        return img_url,summary.strip(), content.strip(), services.strip().replace(' ','‌'), categories.strip().replace(' ','‌'),video_url

    def iso_time_creator(self,date_persian):
        tarikh=date_persian.split()
        date_time_str=tarikh[1].split(':')
        date_time_list=[]
        for i in date_time_str:
            date_time_list.append(int(i))
        if len(date_time_list)==2:
            dt = time1(hour=date_time_list[0], minute=date_time_list[1], microsecond=0)
            return dt.isoformat(timespec='auto')
        elif len(date_time_list)==3:
            dt = time1(hour=date_time_list[0], minute=date_time_list[1], second=date_time_list[2], microsecond=0)
            return dt.isoformat(timespec='auto')
   
    def shamsi_date_creator(self,date_persian):
        tarikh=date_persian.split()
        tarikh_real=tarikh[0].split('-')
        year=tarikh_real[0]
        month=tarikh_real[1]
        day=tarikh_real[2]
        date_shamsi = JalaliDate(int(year),int(month),int(day)).isoformat()
        return date_shamsi

    def process(self, tup):
        self.logger.info("Main url: "+ str(tup.values[0]))

        if tup.values[0] != 'unknown_url':
            main_url = tup.values[0]
            agency = tup.values[1]
            agency_fa = 'خبرآنلاین'
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
            titles=main_soup.find_all('div',{'class':'desc'})
            self.logger.info("Len news: "+str(len(titles)))
            
            for item in titles:
                try:
                    time.sleep(1)
                    title = item.find('h3').get_text()
                    news_url = "https://www.khabaronline.ir/" + str(item.find('a').get('href'))
                    date_persian = str(item.find('time').get_text())
                    date_crawl = datetime.now().isoformat(timespec='seconds') + "Z"
                    img_url,summary,content,services,categories,video_url = self.get_news_content(news_url)
                    date_shamsi = self.shamsi_date_creator(date_persian)
                    date_time= str(date_shamsi) +"T" +self.iso_time_creator(date_persian)
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
                        news_url = "https://khabaronline.ir"

                    self.solr_con.add([
                        {
                            "agency" : agency,
                            "url" : news_url,
                            "news_id" : news_id,
                            "agency_fa":agency_fa,
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
                

