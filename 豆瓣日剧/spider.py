import urllib.request
from multiprocessing import Pool
from urllib.parse import urlencode
import requests
import time
import random
import pymongo
from bs4 import BeautifulSoup
from requests import RequestException
from config import *

myhead = {
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
}

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

def get_page_index(page_number, keyword):
    data = {
        'type': 'tv',
        'tag': keyword,
        'sort': 'rank',
        'page_limit': '20',
        'page_start': page_number
    }
    url = 'https://movie.douban.com/j/search_subjects?' + urlencode(data)
    try:
        response = requests.get(url, headers = myhead)
        if response.status_code == 200:
            data = response.json()
            return data
        return None
    except RequestException:
        print('请求页面失败')
        return None

def parse_page_index(data):
    if data and 'subjects' in data.keys():
        items = data['subjects']
        for item in items:
            yield {
                'url': item.get('url')
            }


def get_page_detail(url, keyword):
    data = {
        'tag': keyword,
        'from': 'gaia'
    }
    myurl = url + '?' + urlencode(data)
    response = urllib.request.Request(myurl)
    response.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36')
    response = urllib.request.urlopen(response)
    html = response.read().decode('utf-8')
    return {'url': myurl,
            'html': html}

def parse_page_detail(url, html):
    soup = BeautifulSoup(html, 'lxml')
    title = soup.h1.find_all('span', property="v:itemreviewed")[0].text.strip()
    release_date = soup.find_all('span', property="v:initialReleaseDate")[0].text.strip()[0 : 10]
    rating = soup.find_all('strong', class_="ll rating_num")[0].text.strip()

    # 类型
    kind = soup.find_all('span', property="v:genre")
    if len(kind) == 0:
        type_ = ''
    elif len(kind) == 1:
        type_ = kind[0].text.strip()
    else:
        type_ = '/'.join(ty.text.strip() for ty in kind)

    # 演员
    actors = soup.find('span', class_="actor").find_all('a')
    if len(actors) == 0:
        actor = ''
    elif len(actors) == 1:
        actor = actors[0].text.strip()
    else:
        actor = '/'.join(actor.text.strip() for actor in actors)

    # 评论
    comments = soup.find('div', id = "hot-comments").find_all('span', class_ = "short")
    if len(comments) == 0:
        comment = ''
    elif len(comments) == 1:
        comment = comments[0].text.strip()
    else:
        comment = '<=>'.join(cm.text.strip() for cm in comments)

    yield {
        'title': title,
        'release_date': release_date,
        'rating': rating,
        'kind': type_,
        'actors': actor,
        'url': url,
        'comments': comment
    }

def save_to_mongo(result):
    try:
        if db[MONGO_TABLE].insert(result):
            print('存储到MONGODB成功', result)
    except Exception:
        print('存储到MONGODB失败', result)

def main(number):
    j_text = get_page_index(number, KEYWORD)
    for item in parse_page_index(j_text):
        url = item['url']
        sleeptime = random.randint(1, 5)
        time.sleep(sleeptime)
        url_2 = get_page_detail(url, KEYWORD)['url']
        html = get_page_detail(url, KEYWORD)['html']
        for i in parse_page_detail(url_2, html):
            save_to_mongo(i)

if __name__ == '__main__':
    groups = [i * 20 for i in range(GROUP_START, GROUP_END + 1)]
    pool = Pool()
    pool.map(main, groups)
