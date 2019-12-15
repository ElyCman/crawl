import pymongo
import requests
from urllib.parse import urlencode, quote
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
import re
from config import *
import json
from multiprocessing import Pool

myheaders = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
}
data = {
    'lang': 'c',
    'postchannel': '0000',
    'workyear': '99',
    'cotype': '99',
    'degreefrom': '99',
    'jobterm': '99',
    'companysize': '99',
    'ord_field': '0',
    'dibiaoid': '0',
    'line': '',
    'welfare': ''
}

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

def get_page_index(url):
    try:
        response = requests.get(url, headers = myheaders)
        if response.status_code == 200:
            response.encoding = 'gbk'
            return response.text
        return None
    except RequestException:
        print('请求索引出错')
        return None

def parse_page_index(html):
    try:
        soup = BeautifulSoup(html, 'lxml')
        items = soup.select('#resultList .el')
        items = items[1 : len(items)]
        pattern = re.compile('<span.*?title="(.*?)">.*?</span', re.S)
        for item in items:
            position = item.find_all(class_ = 't1')[0].text.strip()
            company = re.findall(pattern, str(item.find_all(class_ = 't2')[0]))[0]
            area = item.find_all(class_ = 't3')[0].text
            salary = item.find_all(class_ = 't4')[0].text
            url = item.find_all(class_ = 't1')[0].find_all('a')[0].get('href')
            publish_date = item.find_all(class_ = 't5')[0].text
            yield {
                'position': position,
                'company': company,
                'area': area,
                'salary': salary,
                'publish_date': publish_date,
                'url': url
            }
    except TypeError:
        pass

def get_page_detail(url):
    try:
        soup = BeautifulSoup(get_page_index(url), 'lxml')
        detail_1 = soup.find_all(class_ = 'msg ltype')[0].get('title').replace('\xa0', '').split('|')
        # work
        for w in detail_1:
            if w.find('经验') > 0:
                work_of_years = w
                break
            else:
                work_of_years = ''

        # degree
        for d in detail_1:
            if d in ['初中及以下', '中技', '高中', '中专', '大专', '专科', '本科', '硕士', '博士']:
                degree = d
                break
            else:
                degree = ''

        # wants
        for wt in detail_1:
            if len(re.findall(re.compile('^招.+?人$'), wt)) > 0:
                wants = wt
                break
            else:
                wants = ''

        detail_2 = str(soup.find_all(class_ = 'com_tag')[0])
        pattern = re.compile('<div.*?title="(.*?)"><span.*?title="(.*?)"><.*?title="(.*?)">.*?<span', re.S)
        detail_2_re = re.findall(pattern, detail_2)
        business_nature = detail_2_re[0][0]
        people = detail_2_re[0][1]
        industry = detail_2_re[0][2]

        detail_3 = soup.find_all(class_ = 'bmsg inbox')[0].find_all('p')[0].text
        address = detail_3[5 : ]

        return {
            'work_of_years': work_of_years,
            'degree': degree,
            'wants': wants,
            'business_nature': business_nature,
            'people': people,
            'industry': industry,
            'address': address
        }
    except TypeError:
        pass

def parse_result(html):
    for item in parse_page_index(html):
        yield {
            'position': item['position'],
            'company': item['company'],
            'area': item['area'],
            'salary': item['salary'],
            'publish_date': item['publish_date'],
            'url': item['url'],
            'work_of_years': get_page_detail(item['url'])['work_of_years'],
            'degree': get_page_detail(item['url'])['degree'],
            'wants': get_page_detail(item['url'])['wants'],
            'business_nature': get_page_detail(item['url'])['business_nature'],
            'people': get_page_detail(item['url'])['people'],
            'industry': get_page_detail(item['url'])['industry'],
            'address': get_page_detail(item['url'])['address']
        }

def save_to_mongo(result):
    try:
        if db[MONGO_TABLE].insert(result):
            print('存储到MONGODB成功', result)
    except Exception:
        print('存储到MONGODB失败', result)

def main(num):
    keyword = quote(quote(KEYWORD))
    url = 'https://search.51job.com/list/000000,000000,0000,00,9,99,{0},2,{1}.html?'.format(keyword, num) + urlencode(data)
    html = get_page_index(url)
    for i in parse_result(html):
        #write_to_file(i)
        save_to_mongo(i)

if __name__ == '__main__':
    group = [i for i in range(GROUP_START, GROUP_END + 1)]
    pool = Pool()
    pool.map(main, group)
