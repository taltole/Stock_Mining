"""
Database class:
    handle the database.
    first, can take data after the web scraping and create database with relevant tables.
"""

import pymysql.cursors
from config import *
from Classes import TopMarketScrapper, StockScrapper, IndustryScrapper, SectorScrapper, API_Scrapper


class Database:
    def __init__(self):
        """ connect to database. if don't exists - create database and tables. """

        self.con, self.cur = setup_mysql_db()

    def close_connect_db(self):
        """ close connection to Mysql database. """
        self.con.close()

    def insert_all_to_mysql(self, top_stocks, top_industries, top_sectors):
        """from CSV file, insert all tables:"""

        self.insert_main_table(top_stocks)
        self.insert_industry_table(top_industries)
        self.insert_sectors_table(top_sectors)
        self.insert_valuation_table(top_stocks)
        self.insert_metrics_table(top_stocks)
        self.insert_balance_sheet_table(top_stocks)
        self.insert_price_history_table(top_stocks)
        self.insert_dividends_table(top_stocks)
        self.insert_margins_table(top_stocks)
        self.insert_income_table(top_stocks)

    def insert_main_table(self, top_stocks):
        """ from CSV file, insert Main table to mysql """
        df = top_stocks
        for i, r in df.iterrows():
            sql = """
            INSERT INTO Main (id, Ticker, Last, Change_Percent, Change, Rating, Volume, Mkt_Cap, Price_to_Earnings, 
            EPS, Employees, Sector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            val = [i, r['TICKER'], r['LAST'], r['CHG PERCENT'], r['CHG'], r['RATING'], r['VOL'], r['MKT CAP'], r['P_E'],
                   r['EPS'], r['EMPLOYEES'], r['SECTOR']]
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_industry_table(self, top_industries):
        """ from CSV file, insert Industry table to mysql """
        df = top_industries
        for i, r in df.iterrows():
            sql = """
            INSERT IGNORE INTO Industry (Industry_Name, Mkt_Cap, Dividend_Yield, Change_Percent, 
            Vol, Sector, Stocks) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ;"""
            val = (r['INDUSTRY'], r['MKT CAP'], r['DIV YIELD'], r['CHG PERCENT'], r['VOL'], r['SECTOR'], r['STOCKS'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_sectors_table(self, top_sectors):
        """ from CSV file, insert Sectors table to mysql """
        df = top_sectors
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Sectors (Name, Market_Cap, Dividend_Yield, Change_Percent, " \
                  "Vol, Industries, Stocks) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (r['SECTOR'], r['MKT CAP'], r['DIV YIELD'], r['CHG PERCENT'], r['VOL'], r['INDUSTRIES'], r['STOCKS'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_valuation_table(self, top_stocks):
        """ from CSV file, insert Valuation table to mysql """
        df = top_stocks
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Valuation (ticker_id, Ticker, Market_Cap, Enterprise_Value, " \
                  "Enterprise_Value_to_EBITDA, Total_Shares_Outstanding, Number_of_Employees, Number_of_Shareholders, " \
                  "Price_to_Earnings, Price_to_Revenue, Price_to_Book, Price_to_Sales) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            val = (None, i, r['Ticker'], r['Market Capitalization'], r['Enterprise Value (MRQ)'], r['Enterprise Value/EBITDA (TTM)'],
            r['Total Shares Outstanding (MRQ)'], r['Number of Employees'], r['Number of Shareholders'], r['Price to Earnings Ratio (TTM)'],
               r['Price to Revenue Ratio (TTM)'], r['Price to Book (FY)'], r['Price to Sales (FY)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_metrics_table(self, top_stocks):
        """ from CSV file, insert Metrics table to mysql """
        df = top_stocks
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Metrics (ticker_id, Ticker, Return_on_Assets, Return_on_Equity, " \
                  "Return_on_Invested_Capital, Revenue_per_Employee) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (None, i, r['Ticker'], r['Return on Assets (TTM)'], r['Return on Equity (TTM)'], r['Return on Invested Capital (TTM)'],
            r['Revenue per Employee (TTM)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_balance_sheet_table(self, top_stocks):
        """ from CSV file, insert Balance_Sheet table to mysql """
        # df = top_stocks.iloc[:, 14:20]
        df = top_stocks
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Balance_Sheet (ticker_id, Ticker, Quick_Ratio, Current_Ratio, Debt_to_Equity, " \
                  "Net_Debt, Total_Debt, Total_Assets) VALUES (%s, %s, %s, %s, %s, %s, %s, $s)"
            val = (None, i, r['Ticker'], r['Quick Ratio (MRQ)'], r['Current Ratio (MRQ)'], r['Debt to Equity Ratio (MRQ)'],
                   r['Net Debt (MRQ)'], r['Total Debt (MRQ)'], r['Total Assets (MRQ)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_price_history_table(self, top_stocks):
        """ from CSV file, insert Price_History table to mysql """
        # df = top_stocks.iloc[:, 10:14]
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Price_History (ticker_id, Ticker, Average_Volume_10d, 1_Year_beta, 52_week_high," \
              "52_week_low) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (None, i, r['Ticker'], r['Average Volume (10 day)'], r['1-Year Beta'], r['52 Week High'], r['52 Week Low'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_dividends_table(self, top_stocks):
        """ from CSV file, insert Dividends table to mysql """
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Dividends (ticker_id, Ticker, Dividends_Paid, Dividends_Yield, " \
                  "Dividends_per_Share) VALUES (%s, %s, %s, %s, %s)"
            val = (None, i, r['Ticker'], r['Dividends Paid (FY)'], r['Dividends Yield (FY)'], r['Dividends per Share (FY)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_margins_table(self, top_stocks):
        """ from CSV file, insert Margins table to mysql """
        # df = top_stocks.iloc[:, 27:31]
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Margins (ticker_id, Ticker, Net_Margin, Gross_Margin, Operating_Margin, " \
                  "Pretax_Margin) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (None, i, r['Ticker'], r['Net Margin (TTM)'], r['Gross Margin (TTM)'], r['Operating Margin (TTM)'], r['Pretax Margin (TTM)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_income_table(self, top_stocks):
        """ from CSV file, insert Income table to mysql """
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Income (ticker_id, Ticker, Basic_EPS_FY, Basic_EPS_TTM, EPS_Diluted, Net_Income, " \
                  "EBITDA, Gross_Profit_MRQ, Gross_Profit_FY, Last_Year_Revenue, Total_Revenue, Free_Cash_Flow)" \
              " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (None, i, r['Ticker'], r['Basic EPS (FY)'], r['Basic EPS (TTM)'], r['EPS Diluted (FY)'], r['Net Income (FY)'],
               r['EBITDA (TTM)'], r['Gross Profit (MRQ)'], r['Gross Profit (FY)'], r['Last Year Revenue (FY)'],
                   r['Total Revenue (FY)'], r['Free Cash Flow (TTM)'])

            self.cur.execute(sql, val)
        self.con.commit()

    def read_from_db(self, table):
        """ read and print from Mysql database by statement.
            params:       Columns(str),
                          Table(str),
                optional: Where(column(str),value(str)  """

        self.cur.execute("SELECT * FROM {};".format(table))
        # if where:
        #     self.cur.execute("SELECT {} FROM {} WHERE {}='{}'".format(columns, table, where[0], where[1]))
        # else:
        #     self.cur.execute("SELECT {} FROM {} ".format(columns, table))

        result = self.cur.fetchall()

        return result


# ########## static functions ############


def read_csv(file):
    """ read csv file to DataFrame of pandas package. """

    df = pd.read_csv(file)
    df = df.fillna("empty")  # fillna beacause the python can't pass null to mysql db
    return df


def setup_mysql_db():
    """ connect to mysql server. and create database and tables if don't exists."""

    con = pymysql.Connect(host='localhost',
                          user='root',
                          password='12345678',
                          db='Stock_Stats',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)

    # create if don't exists:
    create_database(con)
    create_tables(con)

    return con, con.cursor()


def create_database(con):
    """ create database if don't exists. """
    cur = con.cursor()
    cur.execute('''DROP DATABASE IF EXISTS Stock_Stats;''')
    cur.execute(''' CREATE DATABASE IF NOT EXISTS Stock_Stats;''')
    cur.execute(''' USE Stock_Stats; ''')


def create_tables(con):
    """ create tables if don't exists. """

    cur = con.cursor()

    create_Main = ''' 
    CREATE TABLE IF NOT EXISTS Main (
    `id` INT PRIMARY KEY AUTO_INCREMENT, 
    `Ticker` VARCHAR(255), 
    `Last` DOUBLE, 
    `Change_Percent` DOUBLE, 
    `Change` DOUBLE, 
    `Rating` VARCHAR(255), 
    `Volume` DOUBLE, 
    `Mkt_Cap` DOUBLE, 
    `Price_to_Earnings` DOUBLE, 
    `EPS` DOUBLE,
    `Employees` DOUBLE, 
    `Sector` VARCHAR(255)
    );'''
    cur.execute(create_Main)

    #############################


    create_Sectors = '''
    CREATE TABLE IF NOT EXISTS `Sectors` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `Name` VARCHAR(255),
    `Market_Cap` DOUBLE,
    `Dividend_Yield` DOUBLE,
    `Change_Percent` DOUBLE,
    `Vol` DOUBLE,
    `Industries` DOUBLE,
    `Stocks` DOUBLE,
     UNIQUE (`Name`)
    );'''

    cur.execute(create_Sectors)
    #############################

    create_Industry = '''
      CREATE TABLE IF NOT EXISTS `Industry` (
      `industry_id` INT PRIMARY KEY AUTO_INCREMENT,
      `Industry_Name` VARCHAR(255),
      `Mkt_Cap` DOUBLE,
      `Dividend_Yield` DOUBLE,
      `change_Percent` DOUBLE,
      `Vol` DOUBLE,
      `Sector` VARCHAR(255),
      `Stocks` DOUBLE
      );'''
    cur.execute(create_Industry)

    #############################

    create_Valuation = '''
    CREATE TABLE IF NOT EXISTS `Valuation` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `ticker_id` INT,
    `Ticker` VARCHAR(255),
    `Market_Capitalization` DOUBLE,
    `Enterprise_Value` DOUBLE,
    `Enterprise_Value_EBITDA` DOUBLE,
    `Total_Shares_Outstanding` DOUBLE,
    `Number_Employees` DOUBLE,
    `Number_Shareholders` DOUBLE,
    `Price_to_Earnings` DOUBLE,
    `Price_to_Revenue` DOUBLE,
    `Price_Book` DOUBLE,
    `Price_Sales` DOUBLE,
     FOREIGN KEY (`Ticker_id`) REFERENCES `Main` (`id`)
        );
        '''
    cur.execute(create_Valuation)

    #############################

    create_Metrics = '''
          CREATE TABLE IF NOT EXISTS `Metrics` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `ticker_id` INT,
          `Ticker` varchar(255),
          `Return_on_Assets` DOUBLE,
          `Return_on_Equity` DOUBLE,
          `Return_on_Invested_Capital` DOUBLE,
          `Revenue_per_Employee` DOUBLE,
           FOREIGN KEY (`Ticker_id`) REFERENCES `Main` (`id`)
        );
        '''
    cur.execute(create_Metrics)

    #############################

    create_Balance_Sheet = '''
          CREATE TABLE IF NOT EXISTS `Balance_Sheet` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `ticker_id` INT,
          `Ticker` varchar(255),
          `Quick_Ratio` DOUBLE,
          `Current_Ratio` DOUBLE,
          `Debt_to_Equity` DOUBLE,
          `Net_Debt` DOUBLE,
          `Total_Debt` DOUBLE,
          `Total_Assets` DOUBLE,
           FOREIGN KEY (`Ticker_id`) REFERENCES `Main` (`id`)
        );
        '''
    cur.execute(create_Balance_Sheet)

    #############################

    create_Price_History = '''
    CREATE TABLE IF NOT EXISTS `Price_History` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `ticker_id` INT,
    `Ticker` varchar(255),
    `Average_Volume_10d` DOUBLE,
    `1_Year_beta` DOUBLE,
    `52_Week_High` DOUBLE,
    `52_Week_Low` DOUBLE,
     FOREIGN KEY (`Ticker_id`) REFERENCES `Main` (`id`)
    );'''
    cur.execute(create_Price_History)

    #############################

    create_Dividends = '''
    CREATE TABLE IF NOT EXISTS `Dividends` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `ticker_id` INT,
    `Ticker` varchar(255),
    `Dividends_Paid` DOUBLE,
    `Dividends_Yield` DOUBLE,
    `Dividends_per_Share` DOUBLE,
     FOREIGN KEY (`Ticker_id`) REFERENCES `Main` (`id`)
    );'''
    cur.execute(create_Dividends)

    #############################

    create_Margins = '''
          CREATE TABLE IF NOT EXISTS `Margins` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `ticker_id` INT,
          `Ticker` varchar(255),
          `Net_Margin` DOUBLE,
          `Gross_Margin` DOUBLE,
          `Operating_Margin` DOUBLE,
          `Pretax_Margin` DOUBLE,
           FOREIGN KEY (`Ticker_id`) REFERENCES `Main` (`id`)
        );
        '''
    cur.execute(create_Margins)

    #############################

    create_Income = '''
          CREATE TABLE IF NOT EXISTS `Income` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `ticker_id` INT,    
          `Ticker` varchar(255),
          `Basic_EPS_FY` DOUBLE,
          `Basic_EPS_TTM` DOUBLE,
          `EPS_Diluted` DOUBLE,
          `Net_Income` DOUBLE,
          `EBITDA` DOUBLE,
          `Gross_Profit_MRQ` DOUBLE,
          `Gross_Profit_FY` DOUBLE,
          `Last_Year_Revenue` DOUBLE,
          `Total_Revenue` DOUBLE,
          `Free_Cash_Flow` DOUBLE,
           FOREIGN KEY (`Ticker_id`) REFERENCES `Main` (`id`)
        );
        '''
    cur.execute(create_Income)

    con.commit()


# def main():
#
#     top_sectors = SectorScrapper.SectorScrapper(URL_SECTOR).summarizer()
#     top_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY).summarizer()
#     top_market = TopMarketScrapper.TopMarketScrapper(URL).summarizer()
#
#     db = Database()
#     db.insert_all_to_mysql(top_market, top_industries, top_sectors)
#     print(db.read_from_db('Main'))
#
#
#     db.close_connect_db()
#     # print("Done. ")
#
#
# if __name__ == "__main__":
#     main()
"""
Database class:
    handle the database.
    first, can take data after the web scraping and create database with relevant tables.
"""

import pymysql.cursors
from config import *


class Database:
    def __init__(self):
        """ connect to database. if don't exists - create database and tables. """

        self.con, self.cur = setup_mysql_db()

    def close_connect_db(self):
        """ close connection to Mysql database. """
        self.con.close()

    def insert_all_to_mysql(self, top_stocks, top_industries, top_sectors):
        """from CSV file, insert all tables:"""

        self.insert_main_table(top_stocks)
        self.insert_industry_table(top_industries)
        self.insert_sectors_table(top_sectors)
        self.insert_valuation_table(top_stocks)
        self.insert_metrics_table(top_stocks)
        self.insert_balance_sheet_table(top_stocks)
        self.insert_price_history_table(top_stocks)
        self.insert_dividends_table(top_stocks)
        self.insert_margins_table(top_stocks)
        self.insert_income_table(top_stocks)

    def insert_main_table(self, top_stocks):
        """ from CSV file, insert Main table to mysql """
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Main (Ticker, Last, Change Percent, Change, Rating, Volume, Mkt Cap, " \
                  "Price to Earnings, EPS, Employees, Sector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (r['TICKER'], r['LAST'], r['CHG PERCENT'], r['CHG'], r['RATING'], r['VOL'], r['MKT CAP'],
               r['P_E'], r['EPS'], r['EMPLOYEES'], r['SECTOR'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_industry_table(self, top_industries):
        """ from CSV file, insert Industry table to mysql """
        df = top_industries
        for i, r in df.iterrows():
            sql = """
            INSERT IGNORE INTO Industry (Industry_Name, Mkt_Cap, Dividend_Yield, Change_Percent, 
            Vol, Sector, Stocks) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ;"""
            val = (r['INDUSTRY'], r['MKT CAP'], r['DIV YIELD'], r['CHG PERCENT'], r['VOL'], r['SECTOR'], r['STOCKS'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_sectors_table(self, top_sectors):
        """ from CSV file, insert Sectors table to mysql """
        df = top_sectors
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Sectors (Name, Market_Cap, Dividend_Yield, Change_Percent, " \
                  "Vol, Industries, Stocks) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (r['SECTOR'], r['MKT CAP'], r['DIV YIELD'], r['CHG PERCENT'], r['VOL'], r['INDUSTRIES'], r['STOCKS'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_valuation_table(self, top_stocks):
        """ from CSV file, insert Valuation table to mysql """
        # df = top_stocks.iloc[:, :10]
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Valuation (Ticker, Market Cap, Enterprise Value, Enterprise Value to EBITDA," \
              "Total_Shares_Outstanding, Number of Employees, Number of Shareholders, Price to Earnings," \
              "Price to Revenue, Price to Book, Price to Sales) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            val = (r['Ticker'], r['Market Capitalization'], r['Enterprise Value (MRQ)'], r['Enterprise Value/EBITDA (TTM)'],
            r['Total Shares Outstanding (MRQ)'], r['Number of Employees'], r['Number of Shareholders'], r['Price to Earnings Ratio (TTM)'],
               r['Price to Revenue Ratio (TTM)'], r['Price to Book (FY)'], r['Price to Sales (FY)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_metrics_table(self, top_stocks):
        """ from CSV file, insert Metrics table to mysql """
        # df = top_stocks.iloc[:, 23:27]
        df = top_stocks
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Metrics (Ticker, Return on Assets, Return on Equity, Return on Invested Capital," \
              "Revenue per Employee) VALUES (%s, %s, %s, %s, %s)"
            val = (r['Ticker'], r['Return on Assets (TTM)'], r['Return on Equity (TTM)'], r['Return on Invested Capital (TTM)'],
            r['Revenue per Employee (TTM)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_balance_sheet_table(self, top_stocks):
        """ from CSV file, insert Balance_Sheet table to mysql """
        # df = top_stocks.iloc[:, 14:20]
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Balance_Sheet (Ticker, Quick Ratio, Current Ratio, Debt to Equity, Net Debt" \
              "Total Debt, Total Assets) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (r['Ticker'], r['Quick Ratio (MRQ)'], r['Current Ratio (MRQ)'], r['Debt to Equity Ratio (MRQ)'], r['Net Debt (MRQ)'],
            r['Total Debt (MRQ)'], r['Total Assets (MRQ)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_price_history_table(self, top_stocks):
        """ from CSV file, insert Price_History table to mysql """
        # df = top_stocks.iloc[:, 10:14]
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Price_History (Ticker, Average_Volume_10d, 1_Year_beta, 52_week_high," \
              "52_week_low) VALUES (%s, %s, %s, %s, %s)"
            val = (r['Ticker'], r['Average Volume (10 day)'], r['1-Year Beta'], r['52 Week High'], r['52 Week Low'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_dividends_table(self, top_stocks):
        """ from CSV file, insert Dividends table to mysql """
        # df = top_stocks.iloc[:, 20:23]
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Dividends (Ticker, Dividends Paid, Dividends Yield, Dividends per Share)" \
              " VALUES (%s, %s, %s, %s)"
            val = (r['Ticker'], r['Dividends Paid (FY)'], r['Dividends Yield (FY)'], r['Dividends per Share (FY)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_margins_table(self, top_stocks):
        """ from CSV file, insert Margins table to mysql """
        # df = top_stocks.iloc[:, 27:31]
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Margins (Ticker, Net Margin, Gross Margin, Operating Margin, Pretax Margin)" \
              " VALUES (%s, %s, %s, %s, %s)"
            val = (r['Ticker'], r['Net Margin (TTM)'], r['Gross Margin (TTM)'], r['Operating Margin (TTM)'], r['Pretax Margin (TTM)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_income_table(self, top_stocks):
        """ from CSV file, insert Income table to mysql """
        # df = top_stocks.iloc[:, 31:40]
        df = top_stocks
        print(df)
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Income (Ticker, Basic EPS FY, Basic EPS TTM, EPS Diluted, Net Income, EBITDA," \
              "Gross Profit MRQ, Gross Profit FY, Last Year Revenue, Total Revenue, Free Cash Flow)" \
              " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (r['Ticker'], r['Basic EPS (FY)'], r['Basic EPS (TTM)'], r['EPS Diluted (FY)'], r['Net Income (FY)'],
               r['EBITDA (TTM)'], r['Gross Profit (MRQ)'], r['Gross Profit (FY)'], r['Last Year Revenue (FY)'], r['Total Revenue (FY)'],
               r['Free Cash Flow (TTM)'])

            self.cur.execute(sql, val)
        self.con.commit()

    def read_from_db(self, table):
        """ read and print from Mysql database by statement.
            params:       Columns(str),
                          Table(str),
                optional: Where(column(str),value(str)  """

        self.cur.execute("SELECT * FROM {}".format(table))
        # if where:
        #     self.cur.execute("SELECT {} FROM {} WHERE {}='{}'".format(columns, table, where[0], where[1]))
        # else:
        #     self.cur.execute("SELECT {} FROM {} ".format(columns, table))

        result = self.cur.fetchall()

        return result


########### static functions ############

def read_csv(file):
    """ read csv file to DataFrame of pandas package. """

    df = pd.read_csv(file)
    df = df.fillna("empty")  # fillna beacause the python can't pass null to mysql db
    return df


def setup_mysql_db():
    """ connect to mysql server. and create database and tables if don't exists."""

    con = pymysql.Connect(host='localhost',
                          user='root',
                          password='Kevin248',
                          db='Stock_Stats',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)

    # create if don't exists:
    create_database(con)
    # create_tables(con)
    return con, con.cursor()


def create_database(con):
    """ create database if don't exists. """
    cur = con.cursor()
    cur.execute('''DROP DATABASE IF EXISTS Stock_Stats;''')
    cur.execute(''' CREATE DATABASE IF NOT EXISTS Stock_Stats;''')
    cur.execute(''' USE Stock_Stats; ''')


def create_tables(con):
    """ create tables if don't exists. """

    cur = con.cursor()

    create_Main = ''' 
    CREATE TABLE IF NOT EXISTS `Main` (
    `id` INT PRIMARY KEY AUTO_INCREMENT, 
    `Ticker` VARCHAR(255), 
    `Last` DOUBLE, 
    `Change_Percent` DOUBLE, 
    `Change` DOUBLE, 
    `Rating` VARCHAR(255), 
    `Volume` DOUBLE, 
    `Mkt_Cap` DOUBLE, 
    `Price_to_Earnings` DOUBLE, 
    `Employees` DOUBLE, 
    `Sector` VARCHAR(255),
     UNIQUE (`Ticker`)
    );'''
    cur.execute(create_Main)

    #############################



    create_Sectors = '''
    CREATE TABLE IF NOT EXISTS `Sectors` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `Name` VARCHAR(255),
    `Market_Cap` DOUBLE,
    `Dividend_Yield` DOUBLE,
    `Change_Percent` DOUBLE,
    `Vol` DOUBLE,
    `Industries` DOUBLE,
    `Stocks` DOUBLE,
     UNIQUE (`Name`)
    );'''

    cur.execute(create_Sectors)
    #############################

    create_Industry = '''
      CREATE TABLE IF NOT EXISTS `Industry` (
      `industry_id` INT PRIMARY KEY AUTO_INCREMENT,
      `Industry_Name` VARCHAR(255),
      `Mkt_Cap` DOUBLE,
      `Dividend_Yield` DOUBLE,
      `change_Percent` DOUBLE,
      `Vol` DOUBLE,
      `Sector` VARCHAR(255),
      `Stocks` DOUBLE
      );'''
    cur.execute(create_Industry)

    #############################

    create_Valuation = '''
    CREATE TABLE IF NOT EXISTS `Valuation` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `Ticker` VARCHAR(255),
    `Market_Capitalization` DOUBLE,
    `Enterprise_Value` DOUBLE,
    `Enterprise_Value_EBITDA` DOUBLE,
    `Total_Shares_Outstanding` DOUBLE,
    `Number_Employees` DOUBLE,
    `Number_Shareholders` DOUBLE,
    `Price_to_Earnings` DOUBLE,
    `Price_to_Revenue` DOUBLE,
    `Price_Book` DOUBLE,
    `Price_Sales` DOUBLE,
     FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`)
        );
        '''
    cur.execute(create_Valuation)

    #############################

    create_Metrics = '''
          CREATE TABLE IF NOT EXISTS `Metrics` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `Ticker` varchar(255),
          `Return_on_Assets` DOUBLE,
          `Return_on_Equity` DOUBLE,
          `Return_on_Invested_Capital` DOUBLE,
          `Revenue_per_Employee` DOUBLE,
           FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`)
        );
        '''
    cur.execute(create_Metrics)

    #############################

    create_Balance_Sheet = '''
          CREATE TABLE IF NOT EXISTS `Balance_Sheet` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `Ticker` varchar(255),
          `Quick_Ratio` DOUBLE,
          `Current_Ratio` DOUBLE,
          `Debt_to_Equity` DOUBLE,
          `Net_Debt` DOUBLE,
          `Total_Debt` DOUBLE,
          `Total_Assets` DOUBLE,
           FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`)
        );
        '''
    cur.execute(create_Balance_Sheet)

    #############################

    create_Price_History = '''
    CREATE TABLE IF NOT EXISTS `Price_History` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `Ticker` varchar(255),
    `Average_Volume_10d` DOUBLE,
    `1_Year_beta` DOUBLE,
    `52_Week_High` DOUBLE,
    `52_Week_Low` DOUBLE,
     FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`)
    );'''
    cur.execute(create_Price_History)

    #############################

    create_Dividends = '''
    CREATE TABLE IF NOT EXISTS `Dividends` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `Ticker` varchar(255),
    `Dividends_Paid` DOUBLE,
    `Dividends_Yield` DOUBLE,
    `Dividends_per_Share` DOUBLE,
     FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`)
    );'''
    cur.execute(create_Dividends)

    #############################

    create_Margins = '''
          CREATE TABLE IF NOT EXISTS `Margins` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `Ticker` varchar(255),
          `Net_Margin` DOUBLE,
          `Gross_Margin` DOUBLE,
          `Operating_Margin` DOUBLE,
          `Pretax_Margin` DOUBLE,
           FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`)
        );
        '''
    cur.execute(create_Margins)

    #############################

    create_Income = '''
          CREATE TABLE IF NOT EXISTS `Income` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `Ticker` varchar(255),
          `Basic_EPS_FY` DOUBLE,
          `Basic_EPS_TTM` DOUBLE,
          `EPS_Diluted` DOUBLE,
          `Net_Income` DOUBLE,
          `EBITDA` DOUBLE,
          `Gross_Profit_MRQ` DOUBLE,
          `Gross_Profit_FY` DOUBLE,
          `Last_Year_Revenue` DOUBLE,
          `Total_Revenue` DOUBLE,
          `Free_Cash_Flow` DOUBLE,
           FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`)
        );
        '''
    cur.execute(create_Income)

    con.commit()


def main():
    db = Database('Stock Info.csv')
    con = setup_mysql_db()[0]
    create_tables(con)
    db.insert_valuation_table()
    print(db.read_from_db('Valuation'))


    db.close_connect_db()
    # print("Done. ")


if __name__ == "__main__":
    main()
