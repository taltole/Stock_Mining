"""
Get top market's stock analysis, stats and updates
"""
# Imports
from config import *


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
        conv_dist = {'K': 10 ** 3, 'M': 10 ** 6, 'B': 10 ** 9}
        stocks = driver.find_elements_by_class_name('tv-data-table__tbody')
        data_list = [i.text for i in stocks][0].split('\n')
        data_len = len(data_list)
        for i in range(0, data_len - 2, 3):
            stock.append(data_list[i][:-1])
            name.append('_'.join(data_list[i + 1].split()))
            info.append(data_list[i + 2].replace('Strong Buy', 'Strong_buy').replace('Strong Sell', 'Strong_Sell')
                        .replace('%', ''))
            # convert values to homogeneous type
            check = info[-1].split()
            for j in range(len(check)):
                try:
                    if not isinstance(check[j], str) or check[j] == '—':
                        check[j] = '0'
                    elif check[j][-1] in conv_dist:
                        num, magnitude = check[j][:-1], check[j][-1]
                        check[j] = str(int(float(num) * conv_dist[magnitude]))
                except IndexError:
                    pass
            info[-1] = ' '.join(check)

            if info[-1].split()[-1] not in ['Finance', 'Communications', 'Transportation', 'Utilities']:
                data.append(info[-1].split()[:-2])
                data[-1].insert(len(data[-1]), '_'.join(info[-1].split()[-2:]))
            else:
                data.append(info[-1].split())

        return stock, name, data

    def summarizer(self):
        """
        sum info in data frame and backup to csv on demand.
        """
        driver.get(URL)
        stock, name, info = self.stock_scrapper()
        [info[i].insert(0, stock[i]) for i in range(len(stock))]
        for ind, row in enumerate(info):
            if len(row) > 11:
                info[ind] = row[:11]

        # get main page headers
        header = ['TICKER', 'LAST', 'CHG PERCENT', 'CHG', 'RATING', 'VOL', 'MKT CAP', 'P_E', 'EPS', 'EMPLOYEES', 'SECTOR']

        # creating data frame
        try:
            df = pd.DataFrame(data=info, columns=header)
        except ValueError:
            print('Data scrapped was corrupted, running again...')
            self.stock_scrapper()

        # Create CSV
        if create_csv:
            filename = 'Stock Info.csv'
            try:
                df.to_csv(PATH_DB + filename, encoding='utf-8', mode='w', header=True)
            except UnboundLocalError:
                pass
        return df


