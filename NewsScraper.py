from IPython.display import display
from IPython.display import clear_output
import plotly.graph_objects as go
from DatabaseManager import DBManager as dm
import StockScraper as ss
import time
from urllib.parse import urljoin
from timeloop import Timeloop
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
#Periodic Loop that keeps code running periodically
periodic_loop = Timeloop()
url = 'https://www.prnewswire.com/news-releases/news-releases-list/'
main_database = dm('NewsStockScraper.db') #database class

def startProgram(): #start the loop
    periodic_loop.start()

def stopProgram(): #stop the time loop
    periodic_loop.stop()

@periodic_loop.job(interval=timedelta(seconds=300))
def parseLatestNews(): #main loop
    print("Scraping Articles....")
    page_html = requests.get(url)
    parsed_object = BeautifulSoup(page_html.content, 'html.parser')
    link_list = parsed_object.find_all('a',class_ = 'news-release')
    link_list = [urljoin(page_html.url,'../../' + item.get('href')) for item in link_list]
    db_urls = main_database.fetchRows('SELECT news_url FROM Articles')
    if db_urls is None:
        main_database.createTable('Articles',['news_url','news_title','news_content','tickers'],['text','text','text','text'])
    else:
        db_urls = [item for tupval in db_urls for item in tupval]
        link_list = [item for item in link_list if item not in db_urls]
    db_rows = []
    if len(link_list) <= 0:
        return
    ticker_article_links = []
    for link in link_list:
        html_of_link = requests.get(link)
        soup_of_link = BeautifulSoup(html_of_link.content, 'html.parser')
        list_of_tickers = soup_of_link.find_all('a',class_='ticket-symbol')
        if len(list_of_tickers) > 0:
            list_of_tickers = [item.string for item in list_of_tickers if item.string]
            list_of_tickers = list(set(list_of_tickers))
            if len(list_of_tickers) <= 0:
                continue
            ticker_article_links.append(link)
            content = soup_of_link.get_text()
            title = soup_of_link.find_all('header',class_='container release-header')
            title = title[0].find_all('h1')
            title = title[0].string
            list_of_tickers = ",".join(list_of_tickers)
            link_dict = (link,title,content,list_of_tickers)
            db_rows.append(link_dict)
    main_database.insertMany('INSERT INTO Articles VALUES(?,?,?,?)',db_rows)
    print("Scraping Articles Completed")
    if len(ticker_article_links) <= 0:
        print("No New Articles")
        return
    article_stock_df = ss.getStockPricesDF(ticker_article_links) #get last five days stocks
    clear_output(wait=True)
    unique_news = article_stock_df.news_title.unique()
    print("******** Stock Visualization ********")
    for news_title in unique_news: #visualize stocks
        print("Headline:",news_title)
        news_df = article_stock_df[article_stock_df.news_title == news_title]
        news_df.sort_values(by='date',inplace=True)
        display(news_df)
        unique_tickers = news_df.ticker.unique()
        for ticker in unique_tickers:
            index_cond = news_df.ticker == ticker
            company_name = news_df.company_name[index_cond].tolist()
            fig = go.Figure()
            fig.add_traces(go.Scatter(x=news_df.date[index_cond], y=news_df.open_price[index_cond], name = "Open Price"))
            fig.add_traces(go.Scatter(x=news_df.date[index_cond], y=news_df.close_price[index_cond], name = "Close Price"))
            fig.update_layout(height=500, width=500, title_text=company_name[0] + " Stock Prices in Last 5 Days")
            fig.show()
            fig = go.Figure()
            fig.add_traces(go.Scatter(x=news_df.date[index_cond], y= news_df.volume[index_cond], name = "Volume"))
            fig.update_layout(height=500, width=500, title_text=company_name[0] + " Stock Volume in Last 5 Days")
            fig.show()
    print("******* Stock Visualization End ********")
            