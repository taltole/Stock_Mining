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
driver = webdriver.Chrome(os.path.join(os.getcwd(), 'chromedriver'), options=chrome_options)
try:
    PATH_DB = os.path.join(os.getcwd().replace('Classes', 'Database'), filename)
except:
    PATH_DB = os.path.join(os.getcwd(), filename)
STOCK = 0
SECTOR = 1
# ARG_NAME = 1
ARG_OPTION = 0
START = 0
END = 1
REQUIRED_NUM_OF_ARGS = 3
DELAY = random.randint(1, 5)
MYSQL_USERNAME = 'root'
