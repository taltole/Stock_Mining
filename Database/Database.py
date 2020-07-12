"""
Database class:
    handle the database.
    first, can take data after the web scraping and create database with relevant tables.
"""

import pymysql.cursors
from DataMining.DataMining.Classes.config import *


class Database:
    def __init__(self, CSV_FILE):
        """ connect to database. if don't exists - create database and tables. """

        self.con, self.cur = setup_mysql_db()
        self.df = read_csv(CSV_FILE)

    def close_connect_db(self):
        """ close connection to Mysql database. """
        self.con.close()

    def insert_all_to_mysql(self):
        """from CSV file, insert all tables:"""

        self.insert_main_table()
        self.insert_industry_table()
        self.insert_sectors_table()
        self.insert_valuation_table()
        self.insert_metrics_table()
        self.insert_balance_sheet_table()
        self.insert_price_history_table()
        self.insert_dividends_table()
        self.insert_margins_table()
        self.insert_income_table()

    def insert_main_table(self):
        """ from CSV file, insert Main table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Main (Ticker, Last, Change Percent, Change, Rating, Volume, Mkt Cap, " \
                  "Price to Earnings, EPS, Employees, Sector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (None, r['TICKER'], r['LAST'], r['CHG PERCENT'], r['CHG'], r['RATING'], r['VOL'], r['MKT CAP'],
               r['P/E'], r['EPS'], r['EMPLOYEES'], r['SECTOR'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_industry_table(self):
        """ from CSV file, insert Industry table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = """
            INSERT IGNORE INTO Industry (industry_id, Industry_Name, Mkt_Cap, Dividend_Yield, Change_Percent, 
            Vol, Sector, Stocks) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ;"""
            val = (None, r['INDUSTRY'], r['MKT CAP'], r['DIV YIELD'], r['CHG PERCENT'], r['VOL'], r['SECTOR'], r['STOCKS'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_sectors_table(self):
        """ from CSV file, insert Sectors table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Sectors (sector_id, Sector Name, Market Cap, Dividend Yield, Change Percent, " \
                  "Vol, Industries, Stocks) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (None, r['SECTOR'], r['MKT CAP'], r['DIV YIELD'], r['CHG PERCENT'], r['VOL'], r['INDUSTRIES'], r['STOCKS'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_valuation_table(self):
        """ from CSV file, insert Valuation table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Valuation (Ticker, Market Cap, Enterprise Value, Enterprise Value to EBITDA," \
              "Total_Shares_Outstanding, Number of Employees, Number of Shareholders, Price to Earnings," \
              "Price to Revenue, Price to Book, Price to Sales ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
              "%s) "
            val = (None, r['Ticker'], r['Market Capitalization'], r['Enterprise Value (MRQ)'], r['Enterprise Value/EBITDA (TTM)'],
            r['Total Shares Outstanding (MRQ)'], r['Number of Employees'], r['Number of Shareholders'], r['Price to Earnings Ratio (TTM)'],
               r['Price to Revenue Ratio (TTM)'], r['Price to Book (FY)'], r['Price to Sales (FY)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_metrics_table(self):
        """ from CSV file, insert Metrics table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Metrics (Ticker, Return on Assets, Return on Equity, Return on Invested Capital," \
              "Revenue per Employee) VALUES (%s, %s, %s, %s, %s)"
            val = (None, r['Ticker'], r['Return on Assets (TTM)'], r['Return on Equity (TTM)'], r['Return on Invested Capital (TTM)'],
            r['Revenue per Employee (TTM)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_balance_sheet_table(self):
        """ from CSV file, insert Balance_Sheet table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Balance_Sheet (Ticker, Quick Ratio, Current Ratio, Debt to Equity, Net Debt" \
              "Total Debt, Total Assets) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (None, r['Ticker'], r['Quick Ratio (MRQ)'], r['Current Ratio (MRQ)'], r['Debt to Equity Ratio (MRQ)'], r['Net Debt (MRQ)'],
            r['Total Debt (MRQ)'], r['Total Assets (MRQ)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_price_history_table(self):
        """ from CSV file, insert Price_History table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Price_History (Ticker, Average Volume (10 days), 1 Year beta, 52 week high," \
              "52 week low) VALUES (%s, %s, %s, %s, %s)"
            val = (
            None, r['Ticker'], r['Average Volume (10 day)'], r['1-Year Beta'], r['52 Week High'], r['52 Week Low'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_dividends_table(self):
        """ from CSV file, insert Dividends table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Dividends (id, Ticker, Dividends Paid, Dividends Yield, Dividends per Share)" \
              " VALUES (%s, %s, %s, %s, %s)"
            val = (None, r['Ticker'], r['Dividends Paid (FY)'], r['Dividends Yield (FY)'], r['Dividends per Share (FY)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_margins_table(self):
        """ from CSV file, insert Margins table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Margins (Ticker, Net Margin, Gross Margin, Operating Margin, Pretax Margin" \
              " VALUES (%s, %s, %s, %s, %s)"
            val = (
            None, r['Ticker'], r['Net Margin (TTM)'], r['Gross Margin (TTM)'], r['Operating Margin (TTM)'], r['Pretax Margin (TTM)'])
            self.cur.execute(sql, val)
        self.con.commit()

    def insert_income_table(self):
        """ from CSV file, insert Income table to mysql """
        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Income (Ticker, Basic EPS FY, Basic EPS TTM, EPS Diluted, Net Income, EBITDA," \
              "Gross Profit MRQ, Gross Profit FY, Last Year Revenue, Total Revenue, Free Cash Flow" \
              " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (None, r['Ticker'], r['Basic EPS (FY)'], r['Basic EPS (TTM)'], r['EPS Diluted (FY)'], r['Net Income (FY)'],
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

    # con = pymysql.connect(host='localhost',
    #                 user='root',
    #                 password='Kevin248',
    #                 db='Stock_Stats',
    #                 charset='utf8mb4',
    #                 cursorclass=pymysql.cursors.DictCursor)
    con = pymysql.Connect(host='localhost',
                          user='root',
                          password='12345678',
                          db='Stock_Stats',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)

    # create if don't exists:
    create_database(con)
    return con, con.cursor()


def create_database(con):
    """ create database if don't exists. """
    cur = con.cursor()
    cur.execute(''' CREATE DATABASE IF NOT EXISTS Stock_Stats''')
    cur.execute(''' USE Stock_Stats ''')


def create_tables(con):
    """ create tables if don't exists. """

    cur = con.cursor()

    create_Main = """
    CREATE TABLE IF NOT EXISTS 'Main' (
    'Id' double PRIMARY KEY AUTO_INCREMENT, 
    'Ticker' varchar(255),
    'Last' int,
    'Change_Percent' float,
    'Change' float,
    'Rating' varchar(255),
    'Volume' int,
    'Mkt_Cap' int,
    'Price_to_Earnings' int,
    'Employees' int,
    'Sector' varchar(255)
    );
    """

    cur.execute(create_Main)

    #############################

    create_Industry = '''
          CREATE TABLE IF NOT EXISTS 'Industry' (
          'industry_id' double PRIMARY KEY AUTO_INCREMENT, 
          'Industry_Name' varchar,
          'Mkt_Cap' varchar,
          'Dividend_Yield' varchar,
          'change_Percent' varchar,
          'Vol' varchar,
          'Sector' varchar(255)
          'Stocks' double
        );
        '''
    cur.execute(create_Industry)

    #############################

    create_Sectors = '''
          CREATE TABLE IF NOT EXISTS 'Sectors' (
          'id' double PRIMARY KEY AUTO_INCREMENT, 
          'Name' varchar(255),
          'Market Cap' float,
          'Dividend Yield' float,
          'Change %' float,
          'Vol' float,
          'Industries' double, 
          'Stocks' double
        );
        '''
    cur.execute(create_Sectors)

    #############################

    create_Valuation = '''
          CREATE TABLE IF NOT EXISTS 'Valuation' (
          'id' double PRIMARY KEY AUTO_INCREMENT, 
          'Ticker' varchar(255),
          'Market_Capitalization' float,
          'Enterprise_Value' float,
          'Enterprise_Value_EBITDA' float,
          'Total_Shares_Outstanding' float,
          'Number_Employees' float,
          'Number_Shareholders' float,
          'Price_to_Earnings' float,
          'Price_to_Revenue' float,
          'Price_Book' float,
          'Price_Sales' float
        );
        '''
    cur.execute(create_Valuation)

    #############################

    create_Metrics = '''
          CREATE TABLE IF NOT EXISTS 'Metrics' (
          'id' double PRIMARY KEY AUTO_INCREMENT,
          'Ticker' varchar(255),
          'Return_on_Assets' float,
          'Return_on_Equity' float,
          'Return_on_Invested_Capital' float,
          'Revenue_per_Employee' float
        );
        '''
    cur.execute(create_Metrics)

    #############################

    create_Balance_Sheet = '''
          CREATE TABLE IF NOT EXISTS 'Balance_Sheet' (
          "id" double PRIMARY KEY AUTO_INCREMENT,
          "Ticker" varchar(255),
          "Quick_Ratio" float,
          "Current_Ratio" float,
          "Debt_to_Equity" float,
          "Net_Debt" float,
          "Total_Debt" float,
          "Total_Assets" float
        );
        '''
    cur.execute(create_Balance_Sheet)

    #############################

    create_Price_History = '''
          CREATE TABLE IF NOT EXISTS 'Price_History' (
          "id" double PRIMARY KEY AUTO_INCREMENT,
          "Ticker" varchar(255),
          "Average_Volume_10d" float,
          "1_Year_beta" float,
          "52_Week_High" float,
          "52_Week_Low" float,
        );
        '''
    cur.execute(create_Price_History)

    #############################

    create_Dividends = '''
          CREATE TABLE IF NOT EXISTS 'Dividends' (
          "id" double PRIMARY KEY AUTO_INCREMENT,
          "Ticker" varchar(255),
          "Dividends_Paid" float,
          "Dividends_Yield" float,
          "Dividends_per_Share" float,
        );
        '''
    cur.execute(create_Dividends)

    #############################

    create_Margins = '''
          CREATE TABLE IF NOT EXISTS 'Margins' (
          "id" double PRIMARY KEY AUTO_INCREMENT,
          "Ticker" varchar(255),
          "Net_Margin" float,
          "Gross_Margin" float,
          "Operating_Margin" float,
          "Pretax_Margin" float,
        );
        '''
    cur.execute(create_Margins)

    #############################

    create_Income = '''
          CREATE TABLE IF NOT EXISTS 'Income' (
          "id" double PRIMARY KEY AUTO_INCREMENT,
          "Ticker" varchar(255),
          "Basic_EPS_FY" float,
          "Basic_EPS_TTM" float,
          "EPS_Diluted" float,
          "Net_Income" float,
          "EBITDA" float,
          "Gross_Profit_MRQ" float,
          "Gross_Profit_FY" float,
          "Last_Year_Revenue" float,
          "Total_Revenue" float,
          "Free_Cash_Flow" float,
        );
        '''
    cur.execute(create_Income)

    #############################
    keys = ''' 
    ALTER TABLE `Income` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`);
    ALTER TABLE `Margins` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`);
    ALTER TABLE `Dividends` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`);
    ALTER TABLE `Price_History` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`);
    ALTER TABLE `Balance_Sheet` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`);
    ALTER TABLE `Metrics` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`);
    ALTER TABLE `Valuation` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`TICKER`);
    ALTER TABLE `Industry` ADD FOREIGN KEY (`Sector`) REFERENCES `Sectors` (`Name`);
    
     '''
    cur.execute(keys, multi=True)
    con.commit()


def main():
    db = Database('Industry info.csv')
    con = setup_mysql_db()
    tables = create_tables(con[1])
    db.insert_industry_table()
    print(db.read_from_db(tables))

    # convert the csv file to tables in database
    # print("Convert CSV to MySQL Database. ")
    # create_tables(con[1])
    # db = Database('Industry info.csv')
    # db.insert_industry_table()

    db.close_connect_db()
    # print("Done. ")


if __name__ == "__main__":
    main()