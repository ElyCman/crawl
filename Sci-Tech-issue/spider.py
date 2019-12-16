from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from config import *
import pymongo

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

browser = webdriver.Chrome()
wait = WebDriverWait(browser, 10)

def search():
    try:
        browser.get('http://star.sse.com.cn/renewal/')
        total = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'body > main > div.album.pb-4.bg-light > div > div:nth-child(2) > div > div:nth-child(2) > div.pagination.hidden-mobile > ul > li:nth-child(10) > a'))
        )
        parse_page()
        return total.text
    except TimeoutException:
        return search()

def next_page(page_number):
    try:
        input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body > main > div.album.pb-4.bg-light > div > div:nth-child(2) > div > div:nth-child(2) > div.pagination.hidden-mobile > ul > li:nth-child(12) > input"))
        )
        submit = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'body > main > div.album.pb-4.bg-light > div > div:nth-child(2) > div > div:nth-child(2) > div.pagination.hidden-mobile > ul > li:nth-child(13) > a'))
        )
        input.clear()
        input.send_keys(page_number)
        submit.click()
        WebDriverWait(browser, 20).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, 'body > main > div.album.pb-4.bg-light > div > div:nth-child(2) > div > div:nth-child(2) > div.pagination.hidden-mobile > ul > li:nth-child(13) > a'))
        )
        parse_page()
    except TimeoutException:
        next_page(page_number)

def parse_page():
    html = browser.page_source
    soup = BeautifulSoup(html, 'lxml')
    items = soup.find_all('tr')
    items = items[1 : len(items) + 1]
    for item in items:
        a = []
        tem = BeautifulSoup(str(item), 'lxml')
        for td in tem.select('td'):
            a.append(td.get_text())
        firm = {
            'issuer': a[0],
            'state': a[1],
            'location': a[2],
            'industry': a[3],
            'sponsor_institution': a[4],
            'law_firm': a[5],
            'accounting_firm': a[6],
            'update_date': a[7],
            'accepting_date': a[8]
        }
        print(firm)
        save_to_mongo(firm)

def save_to_mongo(result):
    try:
        if db[MONGO_TABLE].insert(result):
            print('保存到MONGODB成功', result)
    except Exception:
        print('保存到MONGODB失败', result)

def main():
    total = search()
    total = int(total)
    for i in range(2, total + 1):
        next_page(i)

if __name__ == '__main__':
    main()
