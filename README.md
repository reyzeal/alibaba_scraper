# alibaba_scraper

Alibaba is one of largest and popular e-commerce in entire world. This company has an enormous dataset about products, suppliers and any sort of market's information. We tend to get the data through scraping. Using Python Programming tool we can obtain the data and then stored as spreadsheet file.

## Technique

There are alot of tools and libraries that can be used to crawl website's information. But in this case, we combined Selenium and Requests to get all informations. We tried to make scenario that Selenium as product's link crawler and Requests as scrapper of page product's detail using the links that generated by Selenium.

## Optimalization

We make all of the requests run as worker, which is concurrent and faster than single threaded. Selenium run in the main process of application and all of requests run as threads.

## How to USE

1. Install Python 3
```
https://www.python.org/downloads/
```
2. Install all libraries in `requirements.txt`
```
pip3 install -r requirements.txt
```
3. Download chromedriver
```
https://chromedriver.chromium.org/downloads

or directly for v80.0.3987.16:
https://chromedriver.storage.googleapis.com/index.html?path=80.0.3987.16/
```
After download is completed, place the executable in the same folder of this script.

4. Run main.py for scraping
```
python main.py directory filename url_target worker page_start page_end

page_start : start scraping from N page number
page_end : number, but write none if you want to scrape until the end of result
```

## Output

![Screenshot](https://raw.githubusercontent.com/reyzeal/alibaba_scraper/master/alibaba.PNG)
