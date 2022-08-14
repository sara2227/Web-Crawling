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
        self.logger.info("-------------------Connection made")

    def get_news_content(self, url, headers={'User-Agent': 'Mozilla/5.0'}):
        response = requests.get(url=url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        summary = ''
        if soup.find('strong', {'class': 'news_strong'}):
            summary = soup.find('strong', {'class': 'news_strong'}).get_text()
        elif soup.find('div', {'class': 'pad-subtitle baznashr-subtitle col-xs-36'}):
            summary = soup.find('div', {'class': 'pad-subtitle baznashr-subtitle col-xs-36'}).get_text().strip()

        services = ''
        if soup.find('a', {'class': 'service-news time-color-news'}):
            services = soup.find('a', {'class': 'service-news time-color-news'}).get_text().strip()
        elif soup.find('a', {'class': 'cat-news'}):
            services = soup.find('a', {'class': 'cat-news'}).get_text().strip()

        categories = ''
        if soup.find('a', {'class': 'cat-news-details'}):
            categories = soup.find('a', {'class': 'cat-news-details'}).get_text().strip()
        elif soup.find('a', {'class': 'cat-news time-color-news'}):
            categories = soup.find('a', {'class': 'cat-news time-color-news'}).get_text().strip()

        date_persian = soup.find('div', {'class': "col-sm-18"}).get_text()

        content = ''
        if soup.find('div', {'class': "row baznashr-body"}):
            content = soup.find('div', {'class': "row baznashr-body"}).get_text().strip()

        img_urls = []
        if soup.find('div', {'class': "row image_set news_album_grid_main news_album_main_part photo-grid-album"}):
            gallery = soup.find('div', {
                'class': "row image_set news_album_grid_main news_album_main_part photo-grid-album"}).find_all("a")
            for img in gallery:
                img_urls.append(img.get('href'))
        if soup.find('div', {'class': "parent-lead-img"}):
            img_urls.append(soup.find('div', {'class': "parent-lead-img"}).find("img").get("src"))
        if soup.find('div', {'class': 'row image_set news_album_main_part cont_main_a'}):
            images = soup.find('div', {'class': 'row image_set news_album_main_part cont_main_a'}).find_all('a')
            for img in images:
                img_urls.append(img.get('href'))
        if soup.find("div", {"class": "row album_listi news_album_main_part"}):
            body_images = soup.find("div", {"class": "row album_listi news_album_main_part"}).find_all('img')
            for img in body_images:
                img_urls.append(img.get("src"))
        if soup.find('div', {'class': 'col-xs-36 showcase-content'}):
            album_images = soup.find('div', {'class': 'col-xs-36 showcase-content'}).find_all('img')
            for img in album_images:
                img_urls.append(img.get("src"))

        video_url = ""
        if soup.find('div', {'class': "row videojs-main-container"}):
            video_url = soup.find('div', {'class': "row videojs-main-container"}).find("source").get("src")
        return img_urls, summary.strip(), content.strip(), services.strip().replace(' ','‌'), categories.strip().replace(' ','‌'), video_url, date_persian

    def convert_date(self, input_time):
        persian_numbers = '۱۲۳۴۵۶۷۸۹۰١٢٣٤٥٦٧٨٩٠'
        english_numbers = '12345678901234567890'
        translation_numbers = str.maketrans(persian_numbers, english_numbers)
        time = input_time.strip().translate(translation_numbers)
        convert_dict = {"تير": "04", "فروردين": "01", "ارديبهشت": "02", "فروردین": "01", "اردیبهشت": "02",
                        "خرداد": "03", "تیر": "04", "مرداد": "05", "شهريور": "06", "مهر": "07", "آبان": "08",
                        "آذر": "09", "دی": "10", "بهمن": "11", "اسفند": "12"}
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
        daytime = daytime_match.group()

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
            agency = "yjc"
            agency_fa = "باشگاه‌خبرنگاران‌جوان"
            img_url = []
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
            yjc_archive = main_soup.find('div', {'class': "archive_content"})
            titles = yjc_archive.find_all('div', {"class": "linear_news"})
            self.logger.info("Len news:" + str(len(titles)))

            for item in titles:
                try:
                    time.sleep(1)
                    title = item.find("a").get_text().strip()
                    news_url = "https://www.yjc.news" + item.find("a").get('href')
                    date_crawl = datetime.now().isoformat(timespec='seconds') + "Z"
                    img_url, summary, content, services, categories, video_url, date_persian = self.get_news_content(news_url)
                    date_time = self.convert_date(date_persian)
                    news_id = uuid.uuid5(uuid.NAMESPACE_URL, news_url).hex

                    self.logger.info("News url:  " + str(news_url))
                    # self.logger.info("title:  "+title+"\nservices:  "+services+"\nnews_url:  "+news_url+"\ncategories:  "isna_bolt
                    # +self.categories+"\ndate_persian:  "+date_persian+"\ndate_shamsi:  "+date_shamsi+'T'+date_time+'Z'
                    # +'\ndate_time:  '+date_time+'Z'+"\nsummary:  "
                    # +str(summary)+"\ncontent:  "+str(content)+
                    #         "\nimg  "+ img_url+ "\ndate_crawl:  "+date_crawl+ "\nnews_id:  "+news_id+"\n")
                except:
                    self.logger.info("Get data error." + str(news_url))

                try:
                    if date_time == "" or date_time == None or date_time is None:
                        date_time = "0000-00-00T00:00:00Z"
                    if title == "" or title == None or title is None:
                        title = "title"
                    if services == "" or services == None or services is None:
                        services = 'نامشخص'
                    if categories == "" or categories == None or categories is None:
                        categories = 'متفرقه'
                    if date_persian == "" or date_persian == None or date_persian is None:
                        date_persian = "نا مشخص"
                    if summary == "" or summary == None or summary is None:
                        if not (content == "" or content == None or content is None):
                            summary = content
                        else:
                            summary = "ندارد"
                    if content == "" or content == None or content is None:
                        if not (summary == "" or summary == None or summary is None):
                            content = summary
                        else:
                            content = "ندارد"
                    if news_id == "" or news_id == None or news_id is None:
                        news_id = agency + "_" + str(random.randint(1, 1004663))
                    if news_url == "" or news_url == None or news_url is None:
                        news_url = "https://www.yjc.news"
                    if len(img_url)== 0 or img_url is None:
                        img_url.append("https://www.yjc.news")
                    if video_url == "" or video_url == None or video_url is None:
                        video_url = "ندارد"

                    self.solr_con.add([
                        {
                            "agency": agency,
                            "agency_fa":agency_fa,
                            "url": news_url,
                            "news_id": news_id,
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

