from Classes import TopMarketScrapper, StockScrapper, IndustryScrapper, SectorScrapper, API_Scrapper
from Database.Database import Database
from config import *
from selenium import webdriver
import pandas as pd
import argparse
import sys


def stock_parser():
    """
    The program receives user arguments and prints the info under query.
    """
    parser = argparse.ArgumentParser(
        description="usage: MainScrapper2.py [-h] [concise|expanded] [-ticker_to_scrap]")
    parser.add_argument('scrapper', choices=['concise', 'expanded'], help='select the query to perform')
    parser.add_argument('-ticker_to_scrap', '--ticker', type=str, nargs='?', default='all_stocks',
                        help='choose stock to scrap')
    args = parser.parse_args()
    return args.scrapper, args.ticker


def update_db():

    user_options = stock_parser()[0]
    print("Update Database. ")
    db = Database()

    # Sectors DB
    top_sectors = SectorScrapper.SectorScrapper(URL_SECTOR).summarizer()
    # top_sectors = scrap_sectors
    db.insert_sectors_table(top_sectors)

    # Industry DB
    top_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY).summarizer()
    # top_industries = scrap_industries
    db.insert_industry_table(top_industries)

    # TopMarket DB
    # top_market = TopMarketScrapper.TopMarketScrapper(URL).summarizer()
    # db.insert_main_table(top_market)

    top_stocks = StockScrapper.main(user_options)

    db.insert_valuation_table(top_stocks)
    db.insert_metrics_table(top_stocks)
    db.insert_balance_sheet_table(top_stocks)
    db.insert_price_history_table(top_stocks)
    # db.insert_dividends_table(top_stocks)
    db.insert_margins_table(top_stocks)
    db.insert_income_table(top_stocks)
    db.read_from_db('Industry')

    print("Done. ")
    return db


def main():
    """
    Given a URL of the stock market and the url's for each stock imported from TopMarketScrapper.py,
    creates a printable DataFrame and financial table for all the stocks.
    :return: DF and dict
    """
    db = Database()
    user_options = stock_parser()
    print(user_options[0])
    print(user_options[1])
    if user_options[0] == 'concise':
        # printing info to console and file
        scrap_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY)
        top_industries = scrap_industries.summarizer()
        print('Industry Summary', top_industries, sep='\n')
        db.insert_industry_table(top_industries)
        # printing info to console and file

        scrap_sectors = SectorScrapper.SectorScrapper(URL_SECTOR)
        top_sectors = scrap_sectors.summarizer()
        print('Sectors Summary', top_sectors, sep='\n')
        db.insert_sectors_table(top_sectors)

        # Stock financial in depth info

        # getting urls for individual stock and sectors mining
        scrap_top = TopMarketScrapper.TopMarketScrapper(URL)
        stock, sectors = scrap_top.get_urls()
        # print('Links to Stocks and Sectors:', stock, sectors, sep='\n')
        # printing info to console and file
        top_stocks = scrap_top.summarizer()
        print('', 'Stock Summary', top_stocks, sep='\n')
        db.insert_main_table(top_stocks)

    elif user_options[0] == 'expanded':
        scrap_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY)
        top_industries = scrap_industries.summarizer()
        scrap_industries.create_csv()
        print('Industry Summary', top_industries, sep='\n')
        db.insert_industry_table(top_industries)

        scrap_sectors = SectorScrapper.SectorScrapper(URL_SECTOR)
        top_sectors = scrap_sectors.summarizer()
        scrap_sectors.create_csv()
        print('Sectors Summary', top_sectors, sep='\n')
        # db.insert_sectors_table(top_sectors)

        top_stocks = StockScrapper.main(user_options[1])
        print('Stock Summary', top_stocks, sep='\n')
        db.insert_valuation_table(top_stocks)
        db.insert_metrics_table(top_stocks)
        db.insert_balance_sheet_table(top_stocks)
        db.insert_price_history_table(top_stocks)
        db.insert_dividends_table(top_stocks)
        db.insert_margins_table(top_stocks)
        db.insert_income_table(top_stocks)

        api_overview = API_Scrapper.api_overview(user_options[1])
        print('API Summary', api_overview, sep='\n')
    db.close_connect_db()

    # db = update_db()

if __name__ == '__main__':
    main()
