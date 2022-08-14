from streamparse import Bolt
import requests
from bs4 import BeautifulSoup
import time
import pysolr
from datetime import datetime
import re
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import uuid
import random


class WordCountBolt(Bolt):
    # outputs = ["agency", "url","url_img", "categories", "services", "summary", "title", "content","date_crawl","date","date_persian","news_id"]

    def initialize(self, conf, ctx):
        self.solr_con = pysolr.Solr("http://192.168.99.155:8983/solr/news_core", always_commit=True, timeout=100)
        self.logger.info("Connection made")

    def get_news_content(self, url, headers={'User-Agent': 'Mozilla/5.0'}):
        response = requests.get(url=url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        summary = ''
        if soup.find('p', {'class': 'summary introtext'}):
            summary = soup.find('p', {'class': 'summary introtext'}).get_text().strip()
        elif soup.find('p', {'class': 'introtext'}):
            summary = soup.find('p', {'class': 'introtext'}).get_text().strip()

        services = ''
        categories = ''
        if soup.find('meta', {'property': 'article:section'}):
            subject = soup.find('meta', {'property': 'article:section'}).get('content').split(">")
            services = subject[0].strip().replace(' ','‌')
            if len(subject) > 1:
                categories = subject[1].strip().replace(' ','‌')

        content = ''
        if soup.find('div', {'class': 'item-text'}):
            content = soup.find('div', {'class': 'item-text'}).get_text()

        img_urls = []
        if soup.find('section', {'class': "box content story photoGall"}):
            gallery = soup.find('section', {'class': "box content story photoGall"}).find_all("a")
            for img in gallery:
                img_url = img.get('href')
                img_urls.append(img_url)

        elif soup.find('div', {'class': 'item-text'}):
            body_images = soup.find('div', {'class': 'item-text'}).find_all('img')
            if len(body_images) != 0:
                for image in body_images:
                    img_urls.append(image.get('src'))

            if soup.find('figure', {'class': "item-img"}):
                url = soup.find('figure', {'class': "item-img"}).find('img').get('src')
                img_urls.append(url)

        video_url = ""
        if soup.find('div', {'class': 'video-download'}):
            video_url = soup.find('div', {'class': 'video-download'}).find('a').get("href")
        elif soup.find('div', {'class': 'item-text'}):
            if soup.find('div', {'class': 'item-text'}).find('audio'):
                video_url = soup.find('div', {'class': 'item-text'}).find('audio').get('poster')
            elif soup.find('div', {'class': 'item-text'}).find('video'):
                video_url = soup.find('div', {'class': 'item-text'}).find('video').find('source').get('src')
            elif soup.find('div', {'class': 'item-text'}).find('iframe'):
                video_url = soup.find('iframe').get('src')

        return img_urls, summary.strip(), content.strip(), services, categories, video_url

    def convert_date(self, input_time):
        persian_numbers = '۱۲۳۴۵۶۷۸۹۰١٢٣٤٥٦٧٨٩٠'
        english_numbers = '12345678901234567890'
        translation_numbers = str.maketrans(persian_numbers, english_numbers)
        time = input_time.strip().translate(translation_numbers)
        convert_dict = {"تير": "04", "فروردين": "01", "ارديبهشت": "02", "فروردین": "01", "اردیبهشت": "02",
                        "خرداد": "03", "تیر": "04", "مرداد": "05", "شهريور": "06", "شهريور": "06", "مهر": "07",
                        "آبان": "08",
                        "آذر": "09", "دی": "10", "دی": "10", "بهمن": "11", "اسفند": "12"}
        days = ["شنبه", "یکشنبه", "دوشنبه", "سه شنبه", "چهارشنبه", "پنجشنبه", "جمعه", "سه‌شنبه"]

        temp = time.split()
        time_list = []
        for wrd in temp:
            if wrd in days:
                continue
            if wrd.isnumeric():
                wrd = wrd.zfill(2)
            time_list.append(convert_dict.get(wrd, wrd))

        time = ' '.join(time_list)
        daytime_match = re.search(r'\d{2}:\d{2}', time)
        if daytime_match:
            daytime = daytime_match.group()
        else:
            daytime = "00:00"

        match = re.search(r'(\d{2})\D(\d{2})\D(\d{4})', time)
        if match:
            day, month, year = match.group(1), match.group(2), match.group(3)

        else:
            match = re.search(r'(\d{4})\D(\d{2})\D(\d{2})', time)
            day, month, year = match.group(3), match.group(2), match.group(1)

        formated_date = year + "-" + month + "-" + day + "T" + daytime + ":00Z"
        return formated_date

    def process(self, tup):
        self.logger.info("Main url: " + str(tup.values[0]))

        if tup.values[0] != 'unknown_url':
            main_url = tup.values[0]
            agency = tup.values[1]
            img_url = []
            agency_fa = 'آنا'
            video_url = ''
            categories = ''
            services = ''
            summary = ''
            title = ''
            content = ''
            date_crawl = ''
            date_time = ''
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

            main_response = http.get(url=main_url, verify=False, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
            self.logger.info("Main response: " + str(main_response))
            main_soup = BeautifulSoup(main_response.content, 'html.parser')
            archive = main_soup.find('section', {'id': 'box177'})
            titles = []
            if archive:
                titles = archive.find('div').find_all('li')
            self.logger.info("Len news: " + str(len(titles)))

            for item in titles:
                try:
                    time.sleep(1)
                    news_url = "https://www.ana.press" + item.find('a').get('href')
                    title = item.find('h3').find("a").get_text()
                    date_persian = item.find('time').get_text()
                    date_crawl = datetime.now().isoformat(timespec='seconds') + "Z"
                    img_url, summary, content, services, categories, video_url = self.get_news_content(news_url)
                    date_time = self.convert_date(date_persian)
                    news_id = uuid.uuid5(uuid.NAMESPACE_URL, news_url).hex

                    self.logger.info("News url: " + str(news_url))
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
                        news_url = "https://www.ana.press"

                    self.solr_con.add([
                        {
                            "agency": agency,
                            "url": news_url,
                            "news_id": news_id,
                            "agency_fa": agency_fa,
                            "url_img": img_url,
                            "url_video": video_url,
                            "categories": categories,
                            "services": services,
                            "summary": str(summary),
                            "title": title,
                            "content": str(content),
                            "date_crawl": date_crawl,
                            "date": date_time,
                            "date_persian": date_persian,
                        }
                    ])
                except:
                    self.logger.info("Index Solr Error." + str(news_url))
                    continue

