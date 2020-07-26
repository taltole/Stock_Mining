"""
Get top market's stock analysis, stats and updates
"""
# Imports
from DataMining.DataMining.Classes.config import *


class TopMarketScrapper:

    def __init__(self, URL):
        self.URL = URL

    @classmethod
    def get_urls(self):
        """
        function gets urls from main site to read by other classes
        """
        driver.get(URL)

        # getting stocks urls and sectors urls
        stocks = driver.find_elements_by_class_name('tv-data-table__tbody')
        links = [stock.find_elements_by_tag_name('a') for stock in stocks]
        try:
            urls = [link.get_attribute("href") for link in links[1]]
        except:

            urls = [link.get_attribute("href") for link in links[0]]
        sectors_urls = []
        stocks_urls = []
        for i in range(len(urls)):
            if i % 2 == 0:
                stocks_urls.append(urls[i])
            else:
                sectors_urls.append(urls[i])

        return stocks_urls, sectors_urls

    @classmethod
    def stock_scrapper(self):
        """
        this counter will look for kw for each site main_scraper returns
        :return: purchase links, prices, top choices.
        """
        driver.get(URL)

        # getting top stocks concise info
        stock = []
        name = []
        info = []
        data = []
        stocks = driver.find_elements_by_class_name('tv-data-table__tbody')
        data_list = [i.text for i in stocks][0].split('\n')
        data_len = len(data_list)
        for i in range(0, data_len - 2, 3):
            stock.append(data_list[i])
            name.append('_'.join(data_list[i + 1].split()))
            info.append(data_list[i + 2].replace('Strong Buy', 'Strong_Buy').replace('Strong Sell', 'Strong_Sell'))
            if info[-1].split()[-1] not in ['Finance', 'Communications', 'Transportation', 'Utilities']:
                data.append(info[-1].split()[:-2])
                data[-1].insert(len(data[-1]), '_'.join(info[-1].split()[-2:]))
            else:
                data.append(info[-1].split())
        return stock, name, data

    def summarizer(self):
        """
        sum info in data frame
        """
        driver.get(URL)
        stock, name, info = self.stock_scrapper()
        [info[i].insert(0, stock[i]) for i in range(len(stock))]

        # get main page headers
        header = ['TICKER', 'LAST', 'CHG PERCENT', 'CHG', 'RATING', 'VOL', 'MKT CAP', 'P_E', 'EPS', 'EMPLOYEES', 'SECTOR']

        # creating data frame
        df = pd.DataFrame(data=info, columns=header)
        return df

    def create_csv(self):
        """
        get df and output to csv file
        """
        df = self.summarizer()

        # create CSV file
        if not df.empty:
            # if file does not exist write header
            filename = 'Stock Info.csv'
            if not os.path.isfile(PATH_DB+filename):
                df.to_csv(PATH_DB+filename, encoding='utf-8')
            else:  # else it exists so append without writing the header
                df.to_csv(PATH_DB+filename, encoding='utf-8', mode='w', header=True)
