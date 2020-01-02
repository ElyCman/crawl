import random
import re
import time
from multiprocessing import Pool
from urllib.parse import urlencode
import pymongo
import requests
from bs4 import BeautifulSoup
from requests import RequestException
from config import *

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

myhead = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
}

def get_page_index(number, keyword):
    data = {
        'keyword': keyword,
        'from_source': 'nav_search',
        'spm_id_from': '333.851.b_696e7465726e6174696f6e616c486561646572.10',
        'page': number
    }
    try:
        url = 'https://search.bilibili.com/all?' + urlencode(data)
        response = requests.get(url, headers = myhead)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        print('请求页面失败')
        return None

def parse_page_index(html, keyword):
    soup = BeautifulSoup(html, 'lxml')
    soup_list = soup.find_all('li', class_ = "video-item matrix")
    for item in soup_list:
        sleeptime = random.randint(1, 5)
        time.sleep(sleeptime)
        title = item.find('div', class_ = "headline clearfix").text.strip()
        viewer = item.find_all('span', title = "观看")[0].text.strip()
        upload_date = item.find('span', title = "上传时间").text.strip()
        danmu_quantity = item.find('span', title = '弹幕').text.strip()
        url = item.find('a', class_ = "img-anchor").get('href')[2 : ].strip()
        yield {
            'name': keyword,
            'title': title,
            'viewer': viewer,
            'video_upload_date': upload_date,
            'danmu_quantity': danmu_quantity,
            'url': 'https://' + url
        }

def get_page_detail(url):
    try:
        response = requests.get(url, headers = myhead)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        print('请求页面失败', url)
        return None

def parse_page_danmu(html):
    pattern = re.compile('"cid":(\d+?),', re.S)
    cid = re.search(pattern, html).group(1)
    url = 'https://comment.bilibili.com/{0}.xml'.format(int(cid))
    response = requests.get(url, headers = myhead)
    if response.status_code == 200:
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')
        soups = soup.find_all('d')
        danmu = '<=>'.join(i.text.strip() for i in soups)
        return danmu

def save_to_mongo(result):
    if db[MONGO_TABLE].insert_one(result):
        print('存储到MONGODB成功', result)
        return True
    return False

def main(number):
    html = get_page_index(number, KEYWORD)
    for item in parse_page_index(html, KEYWORD):
        url = item['url']
        html_deatail = get_page_detail(url)
        danmu = parse_page_danmu(html_deatail)
        sleeptime = random.randint(1, 5)
        time.sleep(sleeptime)
        result =  {
            'name': item['name'],
            'title' : item['title'],
            'viewer' : item['viewer'],
            'video_upload_date' : item['video_upload_date'],
            'danmu_quantity' : item['danmu_quantity'],
            'url' : item['url'],
            'danmu': danmu
        }
        save_to_mongo(result)

if __name__ == '__main__':
    groups = [x  for x in range(GROUP_START, GROUP_END + 1)]
    pool = Pool()
    pool.map(main, groups)
