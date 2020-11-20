import time
from timeloop import Timeloop
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
periodic_loop = Timeloop()
period = 50
url = 'https://www.prnewswire.com/news-releases/news-releases-list/'
def fetchData():
    periodic_loop.start()
def stopDatafetch():
    periodic_loop.stop()
@periodic_loop.job(interval=timedelta(seconds=period))
def parseAndScrape():
    page_html = requests.get(url)
    parsed_object = BeautifulSoup(page_html.content, 'html.parser')
    print(parsed_object) #we will use this object to fetch news and other stock prices