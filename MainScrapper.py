from DataMining.DataMining.Classes import TopMarketScrapper, StockScrapper, IndustryScrapper
from DataMining.DataMining.Classes.config import *
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def main():
    """
    Given a URL of the stock market and the url's for each stock imported from TopMarketScrapper.py,
    creates a printable DataFrame and financial table for all the stocks.
    :return: DF and dict
    """

    # getting urls for individual stock and sectors mining
    scrap_top = TopMarketScrapper.TopMarketScrapper(URL)
    stock, sectors = scrap_top.get_urls()
    print('Links to Stocks and Sectors:', stock, sectors, sep='\n')

    # printing info to console and filestock
    top_stocks = scrap_top.summarizer()
    scrap_top.to_csv()
    print('', 'Top Stock Summary', top_stocks, sep='\n')

    # printing info to console and file
    scrap_indus = IndustryScrapper.IndustryScrapper(URL_INDUSTRY)
    top_industries = scrap_indus.summarizer()
    scrap_indus.to_csv()
    print('Top Industry Summary', top_industries, sep='\n')

    # Stock financial in depth info
    urls = TopMarketScrapper.TopMarketScrapper(URL).get_urls()[STOCK]
    stocks = TopMarketScrapper.TopMarketScrapper(URL).stock_scrapper()
    list_stocks = stocks[0]
    index_stock = 0
    stock_table = {}
    list_values = []
    list_elements = [[]]
    for url in urls:
        print(len(urls) - index_stock, url)
        DRIVER = os.path.join(os.getcwd(), 'chromedriver')
        driver = webdriver.Chrome(DRIVER, chrome_options=chrome_options)
        table = StockScrapper.StockScrapper(driver)
        driver.get(url)
        list_elements[0].append(table.rating_elements())
        list_values.append(table.rating_values())
        stock_table[list_stocks[index_stock]] = table.financial_table()
        index_stock += 1
    df_table = pd.DataFrame(list_values, index=list_stocks, columns=list_elements[0][0])
    df_table.to_csv()
    print(stock_table)
    print(df_table)


if __name__ == '__main__':
    main()
