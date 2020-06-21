"""
Get top market's stock analysis, stats and updates
"""
# Imports
from DataMining.DataMining.Classes.config import *


class TopMarketScrapper:

    def __init__(self, URL):
        self.URL = URL

    def get_urls(self):
        """
        function gets urls from main site to read by other classes
        """
        driver.get(URL)

        # getting stocks urls and sectors urls
        stocks = driver.find_elements_by_class_name('tv-data-table__tbody')
        links = [stock.find_elements_by_tag_name('a') for stock in stocks]
        urls = [link.get_attribute("href") for link in links[1]]

        sectors_urls = []
        stocks_urls = []
        for i in range(len(urls)):
            if i % 2 == 0:
                stocks_urls.append(urls[i])
            else:
                sectors_urls.append(urls[i])
                
        return stocks_urls, sectors_urls

    def stock_scrapper(self):
        """
        this func will look for kw for each site main_scraper returns
        :return: purchase links, prices, top choices.
        """
        driver.get(URL)

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
        return stock, name, info

    def summarizer(self):
        """
        sum info in data frame
        """
        driver.get(URL)
        stock, name, info = self.stock_scrapper()

        # get main page headers
        header = ['STOCK', 'LAST', 'CHG %', 'CHG', 'RATING', 'VOL', 'MKT CAP', 'P/E']

        # creating data frame
        df = pd.DataFrame(index=stock, data=info, columns=header)
        return df

    def to_csv(self):
        """
        get df and output to csv file
        """
        df = self.summarizer()

        # create CSV file
        if not df.empty:
            # if file does not exist write header
            file_name = '../Stock Info.csv'
            if not os.path.isfile(file_name):
                df.to_csv(file_name, encoding='utf-8')
            else:  # else it exists so append without writing the header
                df.to_csv(file_name, encoding='utf-8', mode='w', header=False)
