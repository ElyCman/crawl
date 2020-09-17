from multiprocessing import Pool
import requests
import re
from urllib.parse import urlencode
from lxml.etree import XMLSyntaxError
from requests.exceptions import ConnectionError
from pyquery import PyQuery as pq
import random
import pymongo
from config import *

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

base_url = 'https://weixin.sogou.com/weixin?'

# get proxy from proxy pool
def get_proxy():
    try:
        response = requests.get(PROXY_POOL_URL)  # your proxy pool
        if response.status_code == 200:
            proxy = response.text
            proxy_pool = {
                'http': 'http://' + proxy
            }
            return proxy_pool
        return None
    except ConnectionError:
        return None

# url function
def get_url(keyword, page):
    data = {
        'query': keyword,
        '_sug_type_': '',
        's_from': 'input',
        '_sug_': 'n',
        'type': '2',
        'page': page,
        'ie': 'utf8'
    }
    url = base_url + urlencode(data)
    return url

# get index text
def get_index(url, proxy):
    base_header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'Connection': 'keep-alive',
        # your cookie
        'Cookie': 'ABTEST=******; SUID=******; SUID=******; weixinIndexVisited=******; SUV=******; SNUID=******; ppinf=******; pprdig=******; sgid=******; ppmdig=******; JSESSIONID=******',
        'Host': 'weixin.sogou.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }
    response = requests.get(url, headers = base_header, proxies = proxy)
    return response.text

# parse index to get links
def parse_index(html):
    doc = pq(html)
    items = doc('.news-box .news-list li .txt-box h3 a').items()
    for item in items:
        item = item.attr('href')
        if 'https://weixin.sogou.com' in item:
            item = item
        else:
            item = 'https://weixin.sogou.com' + item
        yield item

# convert into real link
def transform_k_h_link(url):
    b = int(random.random() * 100) + 1
    a = url.find('url=')
    url = url + "&k=" + str(b) + "&h=" + url[a + 4 + 21 + b: a + 4 + 21 + b + 1]
    return url

# get real link
def get_real_link(index_url, k_h_url, proxy):
    base_header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'Connection': 'keep-alive',
        # your cookie
        'Cookie': 'ABTEST=******; SUID=******; SUID=******; weixinIndexVisited=******; SUV=******; SNUID=******; ppinf=******; pprdig=******; sgid=******; ppmdig=******; JSESSIONID=******',
        'Host': 'weixin.sogou.com',
        'Refere': index_url,
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }
    response = requests.get(k_h_url, headers = base_header, proxies = proxy)
    if response.status_code in (301, 302):
        proxy = get_proxy()
        response = requests.get(k_h_url, headers = base_header, proxies = proxy)
        html = response.text
    else:
        html = response.text
    pattern = re.compile("url \+= '(.*?)'", re.S)
    url_list = re.findall(pattern, html)
    url = ''
    for i in url_list:
        url += i
    url = url.replace('http:', 'https:')
    return url

# gain detail page
def get_detail(url, proxy):
    header = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'cookie': 'rewardsn=******; wxtokenkey=******',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }
    try:
        response = requests.get(url, headers = header, proxies = proxy)
        if response.status_code in (301, 302):
            prox = get_proxy()
            response = requests.get(url, headers = header, proxies = prox)
        return response.text
    except ConnectionError:
        return None

# parse detail page
def parse_detail(html):
    try:
        doc = pq(html)
        title = doc('#activity-name').text()
        nickname = doc('#js_name').text()
        wechat = doc('#js_profile_qrcode > div > p:nth-child(3) > span').text()
        about = doc('#js_profile_qrcode > div > p:nth-child(4) > span').text()
        content = doc('#js_content').text()
        if len(title):
            return {
                'wechat': wechat,
                'nickname': nickname,
                'about': about,
                'title': title,
                'content': content
            }
        else:
            return None
    except XMLSyntaxError:
        return None

def save_to_mongo(data):
    # prevent repeating with update
    if db['articles'].update({'title': data['title']}, {'$set': data}, True):
        print('Saved to Mongo', data['title'])
    else:
        print('Saved to Mongo Failed', data['title'])

def main(page):
    proxy = get_proxy()
    if proxy:
        index_url = get_url(KEYWORD, page)
        html = get_index(index_url, proxy)
        urls = parse_index(html)
        for url in urls:
            transf_url = transform_k_h_link(url)
            article_url = get_real_link(index_url, transf_url, proxy)
            article_html = get_detail(article_url, proxy)
            if article_html:
                article_data = parse_detail(article_html)
                if article_data:
                    save_to_mongo(article_data)

if __name__ == '__main__':
    groups = [x for x in range(GROUP_START, GROUP_END + 1)]
    pool = Pool()
    pool.map(main, groups)
