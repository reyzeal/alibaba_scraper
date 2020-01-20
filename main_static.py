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

CPU = multiprocessing.cpu_count()
######################################################################################################
url = "https://www.alibaba.com/products/2017_punk_gold_plated.html?spm=a2700.galleryofferlist.0.0.6e9c2f1eOocSNn&IndexArea=product_en"
WORKER = 5
FILENAME = os.path.basename('gzhengdian')
DIRECTORY = os.path.abspath('scrap_files')
PAGE_START = 1
PAGES = 1  # None -> until the end, number -> specific page
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
    imgs = element(text, '.inav.util-clearfix img')
    product = {
        'Productlink': url,
        'Title': str(element(text, '.ma-title')[0].getText()).strip()
    }
    if len(imgs) == 0:
        imgs = element(text, '.thumb img')
        print(len(imgs))
    for i, j in enumerate(imgs):
        image = str(j['src'])
        if '.jpg_' in image:
            image = image[:image.index('jpg_') + 3]
        if 'https:' in image or 'http:' in image:
            product.update({
                f'Imagelink{i}': image,
            })
        else:
            product.update({
                f'Imagelink{i}': 'http:' + image,
            })
    company = element(text, '.company-name.company-name-lite-vb')[0]
    product.update({'Sellerlink': company['href']})
    price = element(text, '.ma-ref-price')
    if len(price) == 0:
        moqs = element(text, '.ma-ladder-price-item')
        for i, j in enumerate(moqs[:4]):
            raw_moq = str(j.select('.ma-quantity-range')[0].getText()).strip()
            moq_number = re.findall(r'([\d-]+)', raw_moq)[0]
            moq_text = re.findall(r'([a-zA-Z]+)', raw_moq)[0]
            product.update({
                f'MOQ number {i + 1}': moq_number,
                f'MOQ text {i + 1}': moq_text,
                f'Price {i + 1}': str(j.select('.ma-spec-price')[0].getText()).strip()
            })
    else:
        maref = element(text, '.ma-reference-price')[0].getText().strip()
        refprice = re.findall(r'[$\d.]+$',price[0].getText())[0]
        maref = maref.replace(refprice, '')
        maref = str(re.findall(r'/ (\w+)', maref)[0]).strip()
        product.update({'MOQ number 1': '1', 'MOQ text 1': maref, 'Price 1': refprice})

    dvalues = element(text, '.sku-attr-dl')
    for i in dvalues:
        label_name = str(i.select('.name')[0].getText()).lower()
        if 'size' in label_name:
            for j, val in enumerate(i.select('.sku-attr-val-frame')):
                product.update({f"Size{j + 1}": preprocessing(val.getText()).strip()})
        if 'length' in label_name:
            for j, val in enumerate(i.select('.sku-attr-val-frame')):
                product.update({f"Size{j + 1}": preprocessing(val.getText()).strip()})
        if 'color' in label_name:
            for j, val in enumerate(i.select('.color[title]')[:3]):
                product.update({f"Color{j + 1}": preprocessing(val['title']).strip()})
    parent = element(text, '.do-entry.do-entry-separate')
    attrs = parent[0].select('.attr-name.J-attr-name')
    vals = parent[0].select('.do-entry-item-val')
    attrs = [preprocessing(i.getText()).lower() for i in attrs]
    vals = [preprocessing(i.getText()) for i in vals]
    for i, j in enumerate(
            ['Jewelry Type', 'Jewelry Main Material', 'Main Stone', 'Material', 'Gender', 'Type', 'Plated', 'Style']):
        if j.lower() in attrs:
            product.update({j: vals[attrs.index(j.lower())]})
    return product


def url_filter(turl):
    global url
    if CRAWLER_TYPE == 'general':
        f = 'http://www.alibaba.com/product-detail/'
    else:
        f = re.findall(r'.*/productlist', url)[0].replace('/productlist', '')
        return f + turl
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


head_data = ['ID', 'Title', 'Productlink', 'MOQ number 1', 'MOQ text 1', 'Price 1', 'MOQ number 2', 'MOQ text 2',
             'Price 2', 'MOQ number 3', 'MOQ text 3', 'Price 3', 'MOQ number 4', 'MOQ text 4',
             'Price 4', 'Jewelry Type', 'Jewelry Main Material', 'Main Stone', 'Material', 'Gender', 'Type', 'Style', 'Plated',
             'Size1', 'Size2', 'Size3', 'Minimum MOQ number', 'Minimum MOQ text','Sellerlink','Imagelink']
worker_pool = {}


def worker(val, row, page):
    global worker_pool
    data = []
    if val[3] != '':
        print(f'Page {page}-Worker', f'remaining jobs:{len(lists)}', val[0])
        result = overview(val[0])
        min_moq = str(val[4]).replace('(Min Order)', '')
        number = re.findall(r'([\d-]+)', min_moq)[0]
        text = re.findall(r'([a-zA-Z]+)', min_moq)[0]
        result.update({'Minimum MOQ number': number})
        result.update({'Minimum MOQ text': text})
        if CRAWLER_TYPE == 'general':
            result.update({'ID': re.findall(r'(\d+)\.html', result.get('Productlink'))[0].replace('.html', '')})
        else:
            result.update({'ID': re.findall(r'/product/(\d+)', result.get('Productlink'))[0].replace('/product/', '')})
        data = [row, result]
    x = worker_pool.get(page, [])
    x.append(data)
    worker_pool.update({page: x})


def periodic(lists, page=1):
    global worker_pool
    print(f'=======PAGE {page} STARTED========')
    workbook = xlsxwriter.Workbook(f'{DIRNAME}/temp/{FILENAME}{page}.xlsx')
    worksheet = workbook.add_worksheet()
    row = 1

    while len(lists) > 0:
        while _thread._count() >= WORKER:
            time.sleep(0.2)
        val = lists.pop()
        _thread.start_new_thread(worker, (val, row, page))
        time.sleep(0.1)
        if len(lists) > 0:
            row += 1
    print('Collecting all workers data')
    while _thread._count() > 1:
        time.sleep(0.1)
    print('Writing to disk...')
    head_set = []
    for i in worker_pool.get(page):
        if len(i) == 2:
            for j in i[1].keys():
                head_set.append(j)
    head_set = list(set(head_set))
    offside = 0
    for i, val in enumerate(head_data):
        if 'Imagelink' in val:
            for j in head_set:
                if 'Imagelink' in j:
                    worksheet.write(0, i+offside, 'Imagelink')
                    offside+=1
        else:
            worksheet.write(0, i, val)
    for i in worker_pool.get(page):
        if len(i) == 2:
            offside = 0
            for j, val in enumerate(head_data):
                if 'Imagelink' in val:
                    temp = [k for k in i[1].keys() if 'Imagelink' in k]
                    for k in temp:
                        worksheet.write(i[0], j + offside, i[1].get(k, ''))
                        offside += 1
                elif type(i[1].get(val, '')) is not list:
                    worksheet.write(i[0], j + offside, i[1].get(val, ''))
                else:
                    print(val,i[1].get(val, ''))
    workbook.close()
    print(f"====PAGE {page} DONE====")


page = PAGE_START
status = True
CRAWLER_TYPE = 'shop'


def url_detection(url):
    if re.match(r'(www\.|.*)alibaba\.com/products/', url):
        return 'general'
    return 'shop'

if __name__ == '__main__':
    while status or _thread._count() > 0:
        if PAGES is None:
            pass
        elif page > PAGES:
            status = False
            continue
        url_parsed = parse.urlparse(url)
        params = parse.parse_qs(url_parsed.query)
        print('Get information about url')
        if url_detection(url) == 'general':
            params = [f'{i}={(params.get(i)[0])}' for i in params.keys()]
            params = "&".join(params)
            _target = f'{url_parsed.scheme}://{url_parsed.netloc}{url_parsed.path}?{params}'
            print('Exactly:general type')
        else:
            params = [f'{i}={(params.get(i)[0])}' for i in params.keys()]
            params = "&".join(params)
            _page = f'/productlist-{page}.html'
            _target = f'{url_parsed.scheme}://{url_parsed.netloc}{_page}?{params}'
            print('Exactly:shop type')

        print('crawling page', page, ' of ' + _target)

        print('opening browser')
        driver.get(_target)
        print('Emulating behaviour')
        elements = driver.find_element_by_css_selector('body')
        elements.send_keys(Keys.CONTROL, Keys.END)
        elements.send_keys(Keys.CONTROL + Keys.END)

        time.sleep(2)
        print('Emulating behaviour phase 1')
        elements.send_keys(Keys.CONTROL, Keys.END)
        elements.send_keys(Keys.CONTROL + Keys.END)
        time.sleep(2)
        print('Emulating behaviour phase 2')
        elements.send_keys(Keys.CONTROL, Keys.END)
        elements.send_keys(Keys.CONTROL + Keys.END)
        time.sleep(2)
        print('Injecting scripts...')
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
            # print('SHOP')
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
            # print(offers)
        print('Script injected, collecting information..')
        while _thread._count() != 0:
            time.sleep(1)
        lists = list(zip(links, imgs, titles, offers, orders, companies, countries, years, ratings))
        lists = [[url_filter(i[0])] + list(i)[1:] for i in lists if url_filter(i[0]) is not None]
        print('Information collected')
        with open(f'{DIRNAME}/temp/{FILENAME}{page}.json', 'w') as f:
            json.dump(lists, f)
        x = driver.execute_script(
            'return document.querySelector(".pages-next.disabled") || document.querySelector(".next-btn.next-btn-normal.next-btn-medium.next-pagination-item.next[disabled]")?1:0')
        print('Call the workers')
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
