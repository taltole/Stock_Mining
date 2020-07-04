from DataMining.DataMining.Classes import TopMarketScrapper, StockScrapper, IndustryScrapper, SectorScrapper
from DataMining.DataMining.Classes.config import *
from selenium import webdriver
import pandas as pd
import argparse
import sys


def stock_parser():
    """
    program receives user argument and print the info under query
    for example:
        -sum      -> print top market stock stats.
        -stock "AAPL"   -> print full AAPL stats.
        -ind "Airlines" -> print all industry stock summary.
        -sec "Finance"  -> print all Real Madrid players.
    """
    parser = argparse.ArgumentParser(description=
                                     "Print stocks (sum|stock|ind|sec) stats following CL args")
    parser.add_argument("query", nargs='+', type=str, help="Choose your query you like to check")
    args = parser.parse_args()

    where_record = args.query[ARG_NAME]
    where_field = args.query[ARG_OPTION]

    # Parse arguments
    if len(args.query) != REQUIRED_NUM_OF_ARGS:
        print("usage: ./FILE.py {query name}")
        sys.exit(1)

    # checking inputs valid value
    if where_field not in ['sector', 'industry', 'stock']:
        print('please provide a valid query to look into, as stats for [sector|industry|stock].')

    if not where_record:
        print(f'please provide a valid name for query to look into')


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

    # printing info to console and file
    top_stocks = scrap_top.summarizer()
    scrap_top.create_csv()
    print('', 'Top Stock Summary', top_stocks, sep='\n')

    # printing info to console and file
    scrap_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY)
    top_industries = scrap_industries.summarizer()
    scrap_industries.create_csv()
    print('Top Industry Summary', top_industries, sep='\n')

    # printing info to console and file
    scrap_sectors = SectorScrapper.SectorScrapper(URL_SECTOR)
    top_sectors = scrap_sectors.summarizer()
    scrap_sectors.create_csv()
    print('Top Sectors Summary', top_sectors, sep='\n')

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
