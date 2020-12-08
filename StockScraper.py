from DatabaseManager import DBManager as dm
import pandas as pd
import yfinance as yf
from datetime import datetime
from datetime import timedelta
main_database = dm('NewsStockScraper.db')

def returnDictVal(dictionary,key):
    if key in dictionary.keys():
        return dictionary[key]
    return 'NA'

def parseAndScrape(): #Only Fetch info of tickers not in database
    db_tickers = main_database.fetchRows('SELECT ticker FROM Companies')
    if db_tickers is None:
        main_database.createTable('Companies',['ticker','name','summary','url','sector','industry','country'], ['text','text','text','text','text','text','text'])
        db_tickers = []
    else:
        db_tickers = [item for tupval in db_tickers for item in tupval]
    article_tickers = main_database.fetchRows('SELECT tickers FROM Articles')
    if article_tickers is None:
        article_tickers = []
    else:
        article_tickers = [item for tupval in article_tickers for item in tupval]
        article_tickers = [item for string_list in article_tickers for item in string_list.split(',')]
    company_details = []
    for ticker in article_tickers:
        if ticker in db_tickers:
            continue
        db_tickers.append(ticker)
        ticker_obj = yf.Ticker(ticker)
        ticker_info = ticker_obj.info
        if 'longName' not in ticker_info.keys():
            if 'shortName' not in ticker_info.keys():
                company_name = 'NA'
            else:
                company_name = ticker_info['shortName']
        else:
            company_name = ticker_info['longName']
        company = (ticker, company_name, returnDictVal(ticker_info,'longBusinessSummary'), returnDictVal(ticker_info,'website'), returnDictVal(ticker_info,'sector'), returnDictVal(ticker_info,'industry'), returnDictVal(ticker_info,'country'))
        company_details.append(company)
    main_database.insertMany('INSERT INTO Companies VALUES(?,?,?,?,?,?,?)',company_details)
    main_database.dropTable('StockPrices') #refresh the database
    main_database.createTable('StockPrices',['stock_id','ticker','date','open','high','low','close','adj_close','volume'],['text','text','text','real','real','real','real','real','integer'])
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    five_days = timedelta(days=5)
    five_days_ago = (now - five_days).strftime("%Y-%m-%d")
    ticker_stock = []
    for ticker in db_tickers:
        ticker_data = yf.download(ticker,start=five_days_ago,end=today)
        left_stocks = 5 - ticker_data.shape[0]
        if left_stocks == 5:
            continue
        elif left_stocks > 0:
            days = timedelta(days = (5 + left_stocks))
            some_days_ago = (now-days).strftime("%Y-%m-%d")
            more_data = yf.download(ticker,start=some_days_ago,end=five_days_ago)
            ticker_data = pd.concat([ticker_data,more_data])
            ticker_data.drop_duplicates(inplace=True)
        if ticker_data.shape[0] < 5:
            continue
        for ix,row in ticker_data.iterrows():
            date = ix.strftime("%Y-%m-%d")
            stock_id = ticker + date          
            stock_row = (stock_id,ticker,date,row['Open'],row['High'],row['Low'],row['Close'],row['Adj Close'],row['Volume'])
            ticker_stock.append(stock_row)
    main_database.insertMany('INSERT INTO StockPrices VALUES(?,?,?,?,?,?,?,?,?)',ticker_stock)
    
    
def getStockPricesDF(article_urls):
    parseAndScrape() #refresh dataset and get last 5 days stock
    df_list = []
    for url in article_urls: #fetch news and ticker
        row = main_database.fetchRowsWithWhere("SELECT news_title,tickers FROM Articles WHERE news_url=?",(url,))
        if row is None:
            continue
        tickers = [item for item in row[0][1].split(',')]
        title = row[0][0]
        for ticker in tickers: #fetch company and stock info
            company_row = main_database.fetchRowsWithWhere("SELECT name FROM Companies WHERE ticker=?",(ticker,))
            company_name = company_row[0][0]
            stock_rows = main_database.fetchRowsWithWhere("SELECT date,open,high,low,close,adj_close,volume FROM StockPrices WHERE ticker=?",(ticker,))
            for stock_val in stock_rows: #create dataframe entries
                date = stock_val[0]
                open_val = stock_val[1]
                high_val = stock_val[2]
                low_val = stock_val[3]
                close_val = stock_val[4]
                adj_close_val = stock_val[5]
                volume = stock_val[6]
                stock_entry = {"news_title":title, "ticker":ticker, "company_name":company_name, "date":pd.to_datetime(date), "open_price":open_val, "high_price":high_val, "low_price":low_val, "close_price":close_val, "adj_close_price":adj_close_val, "volume":volume}
                df_list.append(stock_entry)
    return pd.DataFrame(df_list)  #return dataframe