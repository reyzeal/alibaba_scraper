import _thread
import json
import multiprocessing
import os
import re
import time
from urllib import parse

from combine import combine
import requests
import xlsxwriter
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('directory', help="where the directory for temporary files and result file", default=os.path.dirname(__file__))
parser.add_argument('filename', help="filename", default='alibaba')
parser.add_argument('url', help="url target (list of search)", default="https://www.alibaba.com/products/jewelry.html?spm=a2700.galleryofferlist.0.0.d4d11350t176UR&amp;IndexArea=product_en&amp;assessment_company=ASS&amp;moqt=MOQT100&amp;need_cd=N&amp;ta=y&amp;param_order=CAT-ISO9001,CAT-OHSAS18001,CAT-ISO14001,CAT-BSCI&amp;sortType=TRALV&amp;productTag=1200000228&amp;companyAuthTag=ISO9001,OHSAS18001,ISO14001,BSCI")
parser.add_argument('worker', default=5, type=int, help="worker for multithreading requests")
parser.add_argument('start', default=1, type=int, help="start page")
parser.add_argument('end', help="end page (don't assign if scrap until the end of result)")
CPU = multiprocessing.cpu_count()
######################################################################################################
args = parser.parse_args()
url = args.url
WORKER = args.worker
FILENAME = args.filename
if args.directory == '.':
    DIRECTORY = os.path.abspath(os.path.dirname(__file__))
else:
    DIRECTORY = os.path.abspath(args.directory)
PAGE_START = args.start
if str(args.end).lower() == 'none':
    PAGES = None
else:
    PAGES = int(args.end)
######################################################################################################

THREAD = 1
DIRNAME = os.path.join(os.path.dirname(__file__), DIRECTORY)
if not os.path.exists(DIRNAME):
    os.mkdir(DIRNAME)
if not os.path.exists(DIRNAME + '/temp'):
    os.mkdir(DIRNAME + '/temp')

if THREAD <= 0:
    THREAD = 1
if THREAD < CPU:
    CPU = THREAD




def scrap(url):
    global req
    res = req.get(url, headers={
        'user-agent': 'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB7.4; InfoPath.2; SV1; .NET CLR 3.3.69573; WOW64; en-US)'}).text
    return res


def element(text, target):
    soup = BeautifulSoup(text, features='html.parser')
    a = soup.select(target)
    hrefs = [i for i in a]
    return hrefs


def preprocessing(text):
    return str(text).replace('\n', '').replace(':', '')


def overview(url):
    text = scrap(url)
    parent = element(text, '.do-entry.do-entry-separate')
    product = {
        'Productlink': url,
        'Title': str(element(text, '.ma-title')[0].getText()).strip()
    }
    company = element(text, '.company-name.company-name-lite-vb')[0]
    product.update({'Sellerlink': company['href']})
    price = element(text, '.ma-ref-price')
    if len(price) == 0:
        moqs = element(text, '.ma-ladder-price-item')
        for i, j in enumerate(moqs[:4]):
            product.update({
                f'MOQ {i + 1}': str(j.select('.ma-quantity-range')[0].getText()).strip(),
                f'Price {i + 1}': str(j.select('.ma-spec-price')[0].getText()).strip()
            })
    else:
        maref = element(text, '.ma-reference-price')[0].getText().strip()
        refprice = price[0].getText()
        maref = maref.replace(refprice, '')
        maref = str(re.findall(r'/ (\w+)', maref)[0]).strip()
        product.update({'MOQ 1': '1 ' + maref, 'Price 1': refprice})
    imgs = element(text, '.thumb img')
    for i, j in enumerate(imgs[:5]):
        image = str(j['src'])
        if '.jpg_' in image:
            image = image[:image.index('jpg_') + 3]
        product.update({
            f'Imagelink{i}': 'http:' + image,
        })
    dvalues = element(text, '.sku-attr-dl')
    for i in dvalues:
        if 'size' in str(i.select('.name')[0].getText()).lower():
            for j, val in enumerate(i.select('.sku-attr-val-frame')[:3]):
                product.update({f"Size{j + 1}": preprocessing(val.getText()).strip()})

    attrs = parent[0].select('.attr-name.J-attr-name')
    vals = parent[0].select('.do-entry-item-val')
    attrs = [preprocessing(i.getText()).lower() for i in attrs]
    vals = [preprocessing(i.getText()) for i in vals]
    for i, j in enumerate(
            ['Jewelry Type', 'Jewelry Main Material', 'Main Stone', 'Material', 'Gender', 'Type', 'Plated']):
        if j.lower() in attrs:
            product.update({j: vals[attrs.index(j.lower())]})
    return product


def url_filter(turl):
    global url
    if CRAWLER_TYPE == 'general':
        f = 'http://www.alibaba.com/product-detail/'
    else:
        ii = re.findall(r'productlist-\d+.html\?.*',url)[0]
        f = re.findall(r'.*/productlist',url)[0].replace('/productlist','')
        return f+turl
    o = f'http:{turl}'
    if f in o:
        return o
    return None


target = '.organic-gallery-offer-outter.J-offer-wrapper a'
req = requests.session()
ops = Options()
ops.add_argument('--headless')
ops.add_argument('--ignore-certificate-errors')
ops.add_argument('--ignore-ssl-errors')
ops.add_argument("--log-level=3")
driver = webdriver.Chrome(options=ops, service_log_path='NUL')


def execute_script(script, return_bool=False, return_script='return window._items'):
    driver.execute_script(script)
    if return_bool:
        return driver.execute_script(return_script)


head_data = ['ID', 'Title', 'Productlink', 'MOQ 1', 'Price 1', 'MOQ 2', 'Price 2', 'MOQ 3', 'Price 3', 'MOQ 4',
             'Price 4', 'Jewelry Type', 'Jewelry Main Material', 'Main Stone', 'Material', 'Gender', 'Type', 'Plated',
             'Size1', 'Size2', 'Size3', 'Minimum MOQ', 'Imagelink1', 'Imagelink2', 'Imagelink3', 'Imagelink4',
             'Imagelink5', 'Sellerlink']
worker_pool = {}


def worker(val, worksheet, row, page):
    x = worker_pool.get(page, 0)
    worker_pool.update({page: x + 1})
    if val[3] != '':
        print(f'Page {page}-Worker {x + 1}', f'remaining jobs:{len(lists)}', val[0])
        result = overview(val[0])
        result.update({'Minimum MOQ': str(val[4]).replace('(Min Order)', '')})
        if CRAWLER_TYPE == 'general':
            result.update({'ID': re.findall(r'(\d+)\.html', result.get('Productlink'))[0].replace('.html', '')})
        else:
            result.update({'ID': re.findall(r'/product/(\d+)', result.get('Productlink'))[0].replace('/product/', '')})
        for j, val in enumerate(head_data):
            worksheet.write(row, j, result.get(val, ''))
    x = worker_pool.get(page, 0)
    worker_pool.update({page: x - 1})


def periodic(lists, page=1):
    print(f'=======PAGE {page} STARTED========')
    workbook = xlsxwriter.Workbook(f'{DIRNAME}/temp/{FILENAME}{page}.xlsx')
    worksheet = workbook.add_worksheet()
    row = 1
    for i, val in enumerate(head_data):
        worksheet.write(0, i, val)
    while len(lists) > 0:
        while _thread._count() >= WORKER:
            time.sleep(1)
        val = lists.pop()
        _thread.start_new_thread(worker, (val, worksheet, row, page))
        time.sleep(1)
        if len(lists) > 0:
            row += 1
    workbook.close()
    print(f"====PAGE {page} DONE====")


# _thread.start_new_thread(crawl, (url,))

page = PAGE_START
status = True
CRAWLER_TYPE = 'shop'
def url_detection(url):
    if re.match(r'(www\.|.*)alibaba\.com/products/', url):
        return 'general'
    return 'shop'
while status or _thread._count() > 0:
    if PAGES is None:
        pass
    elif page > PAGES:
        status = False
        continue
    url_parsed = parse.urlparse(url)
    params = parse.parse_qs(url_parsed.query)
    if url_detection(url) == 'general':
        params = [f'{i}={(params.get(i)[0])}' for i in params.keys()]
        params = "&".join(params)
        _target = f'{url_parsed.scheme}://{url_parsed.netloc}{url_parsed.path}?{params}'
    else:
        params = [f'{i}={(params.get(i)[0])}' for i in params.keys()]
        params = "&".join(params)
        _page = f'/productlist-{page}.html'
        _target = f'{url_parsed.scheme}://{url_parsed.netloc}{_page}?{params}'

    print('crawling page', page, ' of ' + _target)
    driver.get(_target)
    elements = driver.find_element_by_css_selector('body')
    elements.send_keys(Keys.CONTROL, Keys.END)
    elements.send_keys(Keys.CONTROL + Keys.END)
    time.sleep(2)
    elements.send_keys(Keys.CONTROL, Keys.END)
    elements.send_keys(Keys.CONTROL + Keys.END)
    time.sleep(2)
    elements.send_keys(Keys.CONTROL, Keys.END)
    elements.send_keys(Keys.CONTROL + Keys.END)
    time.sleep(2)
    if url_detection(url) == 'general':
        CRAWLER_TYPE = 'general'
        execute_script(
            'x = document.querySelectorAll(".organic-gallery-offer-outter.J-offer-wrapper");window._y = []; for(i=0;i<x.length;i++){window._y.push(x[i])};')
        execute_script('for(i=0;i<window._y.length;i++){window._y[i].style="position:fixed;top:0;left:0"}')
        links = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){window._items.push(window._y[i].querySelector("a").getAttribute("href"))}',
            True)
        imgs = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){window._items.push(window._y[i].querySelector("img").getAttribute("src"))}',
            True)
        titles = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".organic-gallery-title__content"); if(temp) window._items.push(temp.innerText); else window._items.push("")}',
            True)
        offers = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".gallery-offer-price"); if(temp) window._items.push(temp.innerText); else window._items.push("")}',
            True)
        orders = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".gallery-offer-minorder"); if(temp) window._items.push(temp.innerText); else window._items.push("")}',
            True)
        companies = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".organic-gallery-offer__seller-company"); if(temp) window._items.push(temp.innerText); else window._items.push("")}',
            True)
        countries = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".seller-tag__country"); if(temp) window._items.push(temp.getAttribute("title")); else window._items.push("")}',
            True)
        years = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".seller-tag__year"); if(temp) window._items.push(temp.innerText); else window._items.push("")}',
            True)
        ratings = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".seb-supplier-review__score"); if(temp) window._items.push(temp.innerText); else window._items.push("")}',
            True)
    else:
        print('SHOP')
        execute_script(
            'x = document.querySelectorAll(".icbu-product-card.vertical.large.product-item");window._y = []; for(i=0;i<x.length;i++){window._y.push(x[i])};')
        execute_script('for(i=0;i<window._y.length;i++){window._y[i].style="position:fixed;top:0;left:0"}')
        links = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){window._items.push(window._y[i].querySelector("a").getAttribute("href"))}',
            True)
        imgs = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector("img");if(temp) window._items.push(window._y[i].querySelector("img").getAttribute("src")); else window._items.push("");}',
            True)
        titles = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".title-con"); if(temp) window._items.push(temp.innerText); else window._items.push("")}',
            True)
        offers = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".price"); if(temp) window._items.push(temp.getAttribute("title")); else window._items.push("")}',
            True)
        orders = execute_script(
            'window._items = [];for(i=0;i<window._y.length;i++){temp = window._y[i].querySelector(".moq"); if(temp) window._items.push(temp.getAttribute("title")); else window._items.push("")}',
            True)
        companies = ['' for i in orders]
        countries = ['' for i in orders]
        years = ['' for i in orders]
        ratings = ['' for i in orders]
        print(offers)

    while _thread._count() != 0:
        time.sleep(5)
    lists = list(zip(links, imgs, titles, offers, orders, companies, countries, years, ratings))
    lists = [[url_filter(i[0])] + list(i)[1:] for i in lists if url_filter(i[0]) is not None]

    with open(f'{DIRNAME}/temp/{FILENAME}{page}.json', 'w') as f:
        json.dump(lists, f)
    x = driver.execute_script('return document.querySelector(".pages-next.disabled") || document.querySelector(".next-btn.next-btn-normal.next-btn-medium.next-pagination-item.next[disabled]")?1:0')
    print(f"current thread:{_thread._count()}/{WORKER}")
    _thread.start_new_thread(periodic, (lists, page))
    time.sleep(1)
    if int(x) == 1:
        status = False
        PAGES = page
    page += 1
driver.close()
print("Combine....")
combine(DIRECTORY, FILENAME)
print("DONE")
time.sleep(5)
