from Classes import TopMarketScrapper, StockScrapper, IndustryScrapper, SectorScrapper, API_Scrapper
from Database.Database import Database
from config import *
import argparse


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


def main():
    """
    Given a URL of the stock market and the url's for each stock imported from TopMarketScrapper.py,
    creates a printable DataFrame and financial table for all the stocks.
    :return: DF and dict
    """
    db = Database()
    user_options = stock_parser()
    print(f'{user_options[ARG_SCRAP].title()} Scrapping On {user_options[ARG_TICKER]}...')
    if user_options[ARG_SCRAP] == 'concise':
        top_sectors = SectorScrapper.SectorScrapper(URL_SECTOR).summarizer()
        top_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY).summarizer()
        top_market = TopMarketScrapper.TopMarketScrapper(URL).summarizer()

        db = Database()
        db.insert_all_to_mysql(top_market, top_industries, top_sectors)

        print("Reading From Database.... ")
        for table in ['Main', 'Industry', 'Sectors']:
            print(f'\n\t\t****{table.upper().center(81)}****\n\n{db.read_from_db(table)}')
        print("Done. ")

    elif user_options[ARG_SCRAP] == 'expanded':
        scrap_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY)
        top_industries = scrap_industries.summarizer()
        print('Industry Summary', top_industries, sep='\n')
        db.insert_industry_table(top_industries)

        scrap_sectors = SectorScrapper.SectorScrapper(URL_SECTOR)
        top_sectors = scrap_sectors.summarizer()
        print('Sectors Summary', top_sectors, sep='\n')
        db.insert_sectors_table(top_sectors)

        top_stocks = StockScrapper.main(user_options[1])
        print('Stock Summary', top_stocks, sep='\n')
        # db.insert_valuation_table(top_stocks)
        # print(db.read_from_db('Valuation'))
        # db.insert_metrics_table(top_stocks)
        # print(db.read_from_db('Metrics'))
        db.insert_balance_sheet_table(top_stocks)
        print(db.read_from_db('Balance_Sheet'))
        # db.insert_price_history_table(top_stocks)
        # print(db.read_from_db('Price_History'))
        # db.insert_dividends_table(top_stocks)
        # print(db.read_from_db('Dividends'))
        # db.insert_margins_table(top_stocks)
        # print(db.read_from_db('Margins'))
        # db.insert_income_table(top_stocks)
        # print(db.read_from_db('Income'))

        api_overview = API_Scrapper.api_overview(user_options[1])
        print('API Summary', api_overview, sep='\n')
    db.close_connect_db()

    # db = update_db()


if __name__ == '__main__':
    main()
