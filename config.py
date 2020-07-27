import os
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('start-maximized')
chrome_options.add_argument('disable-infobars')
chrome_options.add_argument("--disable-extensions")
chrome_options.headless = True

# constant
filename = ''
URL = 'https://www.tradingview.com/markets/stocks-usa/market-movers-large-cap/'
URL_SECTOR = 'https://www.tradingview.com/markets/stocks-usa/sectorandindustry-sector/'
URL_INDUSTRY = 'https://www.tradingview.com/markets/stocks-usa/sectorandindustry-industry/'
PATH = '/Users/tal/Dropbox/ITC20/DataMining/DataMining/Database/chromedriver'
driver = webdriver.Chrome(os.path.join(os.getcwd(), PATH), options=chrome_options)
PATH_DB = os.path.join(os.getcwd().replace('Classes', 'Database'), filename)
CSV_FILE = 'Database/Industry info.csv'
STOCK = 0
SECTOR = 1
ARG_SCRAP = 0
ARG_TICKER = 1
REQUIRED_NUM_OF_ARGS = 3
DELAY = random.randint(1, 5)
MYSQL_USERNAME = 'root'
