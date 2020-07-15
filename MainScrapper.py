from DataMining.DataMining.Classes import TopMarketScrapper, StockScrapper, IndustryScrapper, SectorScrapper
from DataMining.DataMining.Classes.config import *
from selenium import webdriver
import pandas as pd
import argparse
import sys


def stock_parser():
    """
    The program receives user arguments and prints the info under query.
    """
    parser = argparse.ArgumentParser(
        description="usage: MainScrapper.py [-h] [ind|sec|all] [from row] [to row]")
    parser.add_argument('scrapper', choices=['ind', 'sec', 'all'], help='select the query to perform')
    parser.add_argument('from_row', type=int, help='row index from which to start')
    parser.add_argument('to_row', type=int, help='row index from which to end')
    args = parser.parse_args()
    return args.scrapper, args.from_row, args.to_row


def main():
    """
    Given a URL of the stock market and the url's for each stock imported from TopMarketScrapper.py,
    creates a printable DataFrame and financial table for all the stocks.
    :return: DF and dict
    """
    user_options = stock_parser()
    if user_options[ARG_OPTION] == 'all':
        # getting urls for individual stock and sectors mining
        scrap_top = TopMarketScrapper.TopMarketScrapper(URL)
        stock, sectors = scrap_top.get_urls()
        print('Links to Stocks and Sectors:', stock, sectors, sep='\n')

        # printing info to console and file
        top_stocks = scrap_top.summarizer(user_options[START], user_options[END])
        scrap_top.create_csv(user_options[START], user_options[END])
        print('', 'Stock Summary', top_stocks, sep='\n')

        # printing info to console and file
        scrap_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY)
        top_industries = scrap_industries.summarizer(user_options[START], user_options[END])
        scrap_industries.create_csv(user_options[START], user_options[END])
        print('Industry Summary', top_industries, sep='\n')

        # printing info to console and file
        scrap_sectors = SectorScrapper.SectorScrapper(URL_SECTOR)
        top_sectors = scrap_sectors.summarizer(user_options[START], user_options[END])
        scrap_sectors.create_csv(user_options[START], user_options[END])
        print('Sectors Summary', top_sectors, sep='\n')

        # Stock financial in depth info
        urls = TopMarketScrapper.TopMarketScrapper(URL).get_urls()[STOCK]
        stocks = TopMarketScrapper.TopMarketScrapper(URL).stock_scrapper()
        list_stocks = stocks[0]
        index_stock = user_options[1]
        stock_table = {}
        list_values = []
        list_elements = [[]]
        for url in urls[user_options[START]:user_options[END]]:
            print(len(urls) - index_stock, url)
            DRIVER = os.path.join(os.getcwd(), 'chromedriver')
            driver = webdriver.Chrome(DRIVER, chrome_options=chrome_options)
            table = StockScrapper.StockScrapper(driver)
            driver.get(url)
            list_elements[0].append(table.rating_elements())
            list_values.append(table.rating_values())
            stock_table[list_stocks[index_stock]] = table.financial_table()
            index_stock += 1
        df_table = pd.DataFrame(list_values[user_options[START]:user_options[END]],
                                index=list_stocks[user_options[START]:user_options[END]], columns=list_elements[0][0])
        df_table.to_csv()
        print(stock_table)
        print(df_table)

    elif user_options[ARG_OPTION] == 'ind':
        scrap_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY)
        top_industries = scrap_industries.summarizer(user_options[START], user_options[END])
        scrap_industries.create_csv(user_options[START], user_options[END])
        print('Industry Summary', top_industries, sep='\n')

    elif user_options[ARG_OPTION] == 'sec':
        scrap_sectors = SectorScrapper.SectorScrapper(URL_SECTOR)
        top_sectors = scrap_sectors.summarizer(user_options[START], user_options[END])
        scrap_sectors.create_csv(user_options[START], user_options[END])
        print('Sectors Summary', top_sectors, sep='\n')


if __name__ == '__main__':
    main()
