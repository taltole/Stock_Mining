# Imports
import os
import random
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Selenium Options arguments
chrome_options = Options()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('start-maximized')
chrome_options.add_argument('disable-infobars')
chrome_options.add_argument("--disable-extensions")
chrome_options.headless = True

# Print Options
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 15)

# Constant
filename = ''
URL = 'https://www.tradingview.com/markets/stocks-usa/market-movers-large-cap/'
URL_SECTOR = 'https://www.tradingview.com/markets/stocks-usa/sectorandindustry-sector/'
URL_INDUSTRY = 'https://www.tradingview.com/markets/stocks-usa/sectorandindustry-industry/'
try:
    driver = webdriver.Chrome(os.path.join(os.getcwd(), 'chromedriver_Linux'), options=chrome_options)
    print('Linux Driver used!')
except OSError:
    print('OSx Driver used!')
    driver = webdriver.Chrome(os.path.join(os.getcwd(), 'chromedriver_Mac'), options=chrome_options)
PATH_DB = os.path.join(os.getcwd()+'/Database', filename)
CSV_FILE = ''
STOCK = 0
SECTOR = 1
ARG_SCRAP = 0
ARG_TICKER = 1
REQUIRED_NUM_OF_ARGS = 3
DELAY = random.randint(1, 5)
MYSQL_USERNAME = 'root'
API_KEY = 'IQGX1FT91GHD48FM'
create_csv = False
