"""
Get stock analysis, stats and updates
"""
import os
import sys
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import requests
import time
# from . import config

chrome_options = Options()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.headless = True

URL = 'https://www.tradingview.com/markets/stocks-usa/market-movers-large-cap/'
DELAY = random.randint(1, 5)
driver = webdriver.Chrome(os.path.join(os.getcwd(), 'chromedriver'))
driver.get(URL)


class StockScrapper:

    def __init__(self, URL):
        self.URL = URL

    def get_urls(self):
        """
        get SUBJECT from cl_reader as variable to look for on google
        :return: review sites
        """

        # getting stock urls and sectors urls
        stocks = driver.find_elements_by_class_name('tv-data-table__tbody')        
        links = [stock.find_elements_by_tag_name('a') for stock in stocks]
        urls = [link.get_attribute("href") for link in links[1]]
        
        sectors_urls = []
        stocks_urls = []
        for i in range(len(urls)):
            if i % 2 == 0:
                sectors_urls.append(urls[i])
            else:
                stocks_urls.append(urls[i])
                
        return stocks_urls, sectors_urls

    def stock_scrapper(self):
        """
        this func will look for kw for each site main_scraper returns
        :return: purchase links, prices, top choices.
        """
        stocks_urls, sectors_urls = self.get_urls()

        # getting top stocks concise info
        stock = []
        name = []
        info = []
        stocks = driver.find_elements_by_class_name('tv-data-table__tbody')        
        data_list = [i.text for i in stocks[1:]][0].split('\n')
        data_len = len(data_list)
        for i in range(0, data_len-2, 3):
            stock.append(data_list[i])
            name.append('_'.join(data_list[i+1].split()))
            info.append(data_list[i+2].replace('Strong Buy', 'Strong_Buy'))
        info = [i.split()[0:8] for i in info]

        # getting top stocks in depth info
        ########

        return stock, name, info

    def summarizer(self):
        """
        sum info in data frame
        """
        # get table inputs
        urls = self.get_urls()[0]
        stock, name, info = self.stock_scrapper()

        # get main page headers
        header = driver.find_element_by_xpath('//*[@id="js-screener-container"]/div[3]/table/thead')
        header = header.text.replace('TICKER\n\n100 matches', 'STOCK').split('\n')

        # creating data frame
        df = pd.DataFrame(index=stock, data=info, columns=header[1:])

        return df

    def to_csv(self):
        """
        get df and output to csv file
        """
        df = self.summarizer()

        # create CSV file
        if not df.empty:
            # if file does not exist write header
            file_name = 'Stock Info.csv'
            if not os.path.isfile(file_name):
                df.to_csv(file_name, encoding='utf-8')
            else:  # else it exists so append without writing the header
                df.to_csv(file_name, encoding='utf-8', mode='a', header=False)

    def cl_reader(self):
        """
        function takes subject and kw from CL and  will parse them
        :return: the parses to function
        """
        pass
    

def main():

    scrap = StockScrapper(URL)

    # getting urls for individual stock and sectors mining
    sites, sectors = scrap.get_urls()
    print(sites, sectors)

    # printing info to console and file
    top_stocks = scrap.summarizer()
    scrap.to_csv()

    print(top_stocks)

    # closing driver
    driver.close()


if __name__ == '__main__':
    main()