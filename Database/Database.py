"""
Database class:
    handle the database.
    first, can take data after the web scraping and create database with relevant tables.
"""
#import mysql.connector

import pymysql.cursors
from config import *
from DataMining.Classes import TopMarketScrapper, StockScrapper, IndustryScrapper, SectorScrapper, API_Scrapper


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
        self.insert_valuation_table(top_stocks, ids_dict)
        self.insert_metrics_table(top_stocks)
        self.insert_balance_sheet_table(top_stocks)
        self.insert_price_history_table(top_stocks)
        self.insert_dividends_table(top_stocks)
        self.insert_margins_table(top_stocks)
        self.insert_income_table(top_stocks)
        self.insert_income_table(api)

    def dict_sectors(self, top_sectors):
        df = top_sectors
        dict_sectors = {}
        for i in range(len(df)):
            dict_sectors[df.iloc[i, 0][:-1]] = i + 1
        return dict_sectors

    def insert_sectors_table(self, top_sectors):
        """ insert Sectors table to mysql """
        try:
            count_sector = self.cur.execute("SELECT sector_id FROM Sectors")
            if count_sector < 20:
                df = top_sectors[:21]
                for i, r in df.iterrows():
                    sql = "INSERT INTO Sectors (Sector, Industries, Stocks) VALUES (%s, %s, %s);"
                    val = (r['SECTOR'], r['INDUSTRIES'], r['STOCKS'])
                    self.cur.execute(sql, val)
                self.con.commit()
        except:
            pass

    def insert_industry_table(self, top_industries, dict_sectors):
        """ insert Industry table to mysql """
        try:
            count_industry = self.cur.execute("SELECT id FROM Industry")
            if count_industry < 129:
                df = top_industries
                for i, r in df.iterrows():
                    sector_id = dict_sectors[r['SECTOR']]
                    sql = """INSERT INTO Industry (sector_id, Industry_Name, Sector, Stocks) VALUES (%s, %s, %s, %s)"""
                    val = (sector_id, r['INDUSTRY'], r['SECTOR'], r['STOCKS'])
                    self.cur.execute(sql, val)
                self.con.commit()
        except:
            pass

        
    def insert_main_table(self, top_stocks, dict_sectors):
        """ insert or update Main table to mysql """
        df = top_stocks
        count_id = self.cur.execute("""SELECT ticker_id FROM Main""")
        if count_id > 0:
            for i, r in df.iterrows():
                sector_id = dict_sectors[' '.join(r['SECTOR'].split('_'))]
                self.cur.execute("""UPDATE Main SET sector_id = %s, Last = %s, Change_Percent = %s,  Rating = %s, 
                Volume = %s, Mkt_Cap = %s, Price_to_Earnings = %s, EPS = %s, Employees = %s, Sector = %s WHERE Ticker = %s""",
                (sector_id, r['LAST'], r['CHG PERCENT'], r['RATING'], r['VOL'], r['MKT CAP'], r['P_E'],
                 r['EPS'], r['EMPLOYEES'], r['SECTOR'], r['TICKER']))
        else:
            for i, r in df.iterrows():
                sector_id = dict_sectors[' '.join(r['SECTOR'].split('_'))]
                sql = """ INSERT INTO Main (sector_id, Ticker, Last, Change_Percent, Rating, Volume, Mkt_Cap, Price_to_Earnings,
                 EPS, Employees, Sector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                val = [sector_id, r['TICKER'], r['LAST'], r['CHG PERCENT'], r['RATING'], r['VOL'], r['MKT CAP'], r['P_E'],
                       r['EPS'], r['EMPLOYEES'], r['SECTOR']]
                self.cur.execute(sql, val)
        self.con.commit()

    def insert_valuation_table(self, top_stocks, ids_list):
        """ insert or update Valuation table to mysql """
        df = top_stocks
        for i, r in df.iterrows():
            for ticker_dict in ids_list:
                if ticker_dict['Ticker'] == i:
                    count_id = self.cur.execute("""SELECT ticker_id FROM Valuation WHERE Ticker = %s""", (i))
                    if count_id > 0:
                        self.cur.execute("""UPDATE Valuation SET ticker_id = %s, 
                            Price_to_Revenue = %s, Price_to_Book = %s, Price_to_Sales = %s
                            WHERE Ticker = %s""", (ticker_dict['ticker_id'],r['Price to Revenue Ratio (TTM)'],
                            r['Price to Book (FY)'], r['Price to Sales (FY)'], i))
                    else:
                        sql = """INSERT INTO Valuation (ticker_id, Ticker, Price_to_Revenue, Price_to_Book, Price_to_Sales) VALUES (%s, %s, %s, %s, %s)"""
                        val = (ticker_dict['ticker_id'], i,
                               r['Price to Revenue Ratio (TTM)'], r['Price to Book (FY)'], r['Price to Sales (FY)'])
                        self.cur.execute(sql, val)
        self.con.commit()

    def insert_metrics_table(self, top_stocks, ids_list):
        """ insert or update Metrics table to mysql """
        try:
            df = top_stocks
            for i, r in df.iterrows():
                for ticker_dict in ids_list:
                    if ticker_dict['Ticker'] == i:
                        count_id = self.cur.execute("""SELECT ticker_id FROM Metrics WHERE Ticker = %s""", (i))
                        if count_id > 0:
                            self.cur.execute("""UPDATE Metrics SET ticker_id = %s, Return_on_Assets = %s, 
                            Return_on_Equity = %s, Return_on_Invested_Capital = %s, Revenue_per_Employee = %s
                            WHERE Ticker = %s""",(ticker_dict['ticker_id'],r['Return on Assets (TTM)'],
                            r['Return on Equity (TTM)'], r['Return on Invested Capital (TTM)'], r['Revenue per Employee (TTM)'], i))
                        else:
                            sql = "INSERT INTO Metrics (ticker_id, Ticker, Return_on_Assets, Return_on_Equity, " \
                                  "Return_on_Invested_Capital, Revenue_per_Employee) VALUES (%s, %s, %s, %s, %s, %s)"
                            val = (ticker_dict['ticker_id'], i, r['Return on Assets (TTM)'], r['Return on Equity (TTM)'],
                                   r['Return on Invested Capital (TTM)'], r['Revenue per Employee (TTM)'])
                            self.cur.execute(sql, val)
            self.con.commit()
        except:
            pass

    def insert_balance_sheet_table(self, top_stocks, ids_list):
        """ insert or update Balance_Sheet table to mysql """
        try:
            df = top_stocks
            for i, r in df.iterrows():
                for ticker_dict in ids_list:
                    if ticker_dict['Ticker'] == i:
                        count_id = self.cur.execute("""SELECT ticker_id FROM Balance_Sheet WHERE Ticker = %s""", (i))
                        if count_id > 0:
                            self.cur.execute("""UPDATE Balance_Sheet SET ticker_id = %s, Quick_Ratio = %s, 
                            Current_Ratio = %s, Debt_to_Equity = %s, Net_Debt = %s, Total_Debt = %s, Total_Assets = %s 
                            WHERE Ticker = %s""",(ticker_dict['ticker_id'], r['Quick Ratio (MRQ)'], r['Current Ratio (MRQ)'],
                            r['Debt to Equity Ratio (MRQ)'], r['Net Debt (MRQ)'], r['Total Debt (MRQ)'], r['Total Assets (MRQ)'], i))
                        else:
                            sql = "INSERT INTO Balance_Sheet (ticker_id, Ticker, Quick_Ratio, Current_Ratio, Debt_to_Equity, " \
                                  "Net_Debt, Total_Debt, Total_Assets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                            val = (ticker_dict['ticker_id'], i, r['Quick Ratio (MRQ)'], r['Current Ratio (MRQ)'],
                                   r['Debt to Equity Ratio (MRQ)'],r['Net Debt (MRQ)'], r['Total Debt (MRQ)'], r['Total Assets (MRQ)'])
                            self.cur.execute(sql, val)
            self.con.commit()
        except:
            pass

    def insert_price_history_table(self, top_stocks, ids_list):
        """ insert or update Price_History table to mysql """
        try:
            df = top_stocks
            for i, r in df.iterrows():
                for ticker_dict in ids_list:
                    if ticker_dict['Ticker'] == i:
                        count_id = self.cur.execute("""SELECT ticker_id FROM Price_History WHERE Ticker = %s""", (i))
                        if count_id > 0:
                            self.cur.execute("""UPDATE Price_History SET ticker_id = %s,  
                            1_Year_beta = %s, 52_week_high = %s, 52_week_low = %s WHERE Ticker = %s""",
                            (ticker_dict['ticker_id'], r['1-Year Beta'], r['52 Week High'],
                             r['52 Week Low'], i))
                        else:
                            sql = "INSERT INTO Price_History (ticker_id, Ticker, 1_Year_beta, 52_week_high," \
                          "52_week_low) VALUES (%s, %s, %s, %s, %s)"
                            val = (ticker_dict['ticker_id'], i, r['1-Year Beta'], r['52 Week High'], r['52 Week Low'])
                            self.cur.execute(sql, val)
            self.con.commit()
        except:
            pass


    def insert_dividends_table(self, top_stocks, ids_list):
        """ insert or update Dividends table to mysql """
        try:
            df = top_stocks
            for i, r in df.iterrows():
                for ticker_dict in ids_list:
                    if ticker_dict['Ticker'] == i:
                        count_id = self.cur.execute("""SELECT ticker_id FROM Dividends WHERE Ticker = %s""", (i))
                        if count_id > 0:
                            self.cur.execute("""UPDATE Dividends SET ticker_id = %s, Dividends_Paid = %s, 
                            Dividends_Yield = %s, Dividends_per_Share = %s, 52_week_low = %s WHERE Ticker = %s""",
                            (ticker_dict['ticker_id'], r['Dividends Paid (FY)'], r['Dividends Yield (FY)'],
                             r['Dividends per Share (FY)'], i))
                        else:
                            sql = "INSERT INTO Dividends (ticker_id, Ticker, Dividends_Paid, Dividends_Yield, " \
                                  "Dividends_per_Share) VALUES (%s, %s, %s, %s, %s)"
                            val = (ticker_dict['ticker_id'], i, r['Dividends Paid (FY)'], r['Dividends Yield (FY)'],
                                   r['Dividends per Share (FY)'])
                            self.cur.execute(sql, val)
            self.con.commit()
        except:
            pass

    def insert_margins_table(self, top_stocks, ids_list):
        """ insert or update Margins table to mysql """
        try:
            df = top_stocks
            for i, r in df.iterrows():
                for ticker_dict in ids_list:
                    if ticker_dict['Ticker'] == i:
                        count_id = self.cur.execute("""SELECT ticker_id FROM Margins WHERE Ticker = %s""", (i))
                        if count_id > 0:
                            self.cur.execute("""UPDATE Margins SET ticker_id = %s, Net_Margin = %s, 
                               Gross_Margin = %s, Operating_Margin = %s, Pretax_Margin = %s WHERE Ticker = %s""",
                                (ticker_dict['ticker_id'], r['Net Margin (TTM)'], r['Gross Margin (TTM)'],
                                r['Operating Margin (TTM)'], r['Pretax Margin (TTM)'], i))
                        else:
                            sql = "INSERT INTO Margins (ticker_id, Ticker, Net_Margin, Gross_Margin, Operating_Margin, " \
                                  "Pretax_Margin) VALUES (%s, %s, %s, %s, %s, %s)"
                            val = (ticker_dict['ticker_id'], i, r['Net Margin (TTM)'], r['Gross Margin (TTM)'],
                                   r['Operating Margin (TTM)'], r['Pretax Margin (TTM)'])
                            self.cur.execute(sql, val)
            self.con.commit()
        except:
            pass

    def insert_income_table(self, top_stocks, ids_list):
        """ insert or update Income table to mysql """
        try:
            df = top_stocks
            for i, r in df.iterrows():
                for ticker_dict in ids_list:
                    if ticker_dict['Ticker'] == i:
                        count_id = self.cur.execute("""SELECT ticker_id FROM Income WHERE Ticker = %s""", (i))
                        if count_id > 0:
                            self.cur.execute("""UPDATE Income SET ticker_id = %s, Basic_EPS_FY = %s, 
                                 Basic_EPS_TTM = %s, EPS_Diluted = %s, EBITDA = %s, Gross_Profit_MRQ = %s, 
                                 Last_Year_Revenue = %s, Total_Revenue = %s, Free_Cash_Flow = %s, WHERE Ticker = %s""",
                                 (ticker_dict['ticker_id'], r['Basic EPS (FY)'], r['Basic EPS (TTM)'],
                                 r['EPS Diluted (FY)'], r['Net Income (FY)'], r['EBITDA (TTM)'],
                                 r['Gross Profit (MRQ)'], r['Gross Profit (FY)'], r['Last Year Revenue (FY)'],
                                 r['Total Revenue (FY)'], r['Free Cash Flow (TTM)'], i))
                        else:
                            sql = "INSERT INTO Income (ticker_id, Ticker, Basic_EPS_FY, Basic_EPS_TTM, EPS_Diluted, Net_Income, " \
                                  "EBITDA, Gross_Profit_MRQ, Gross_Profit_FY, Last_Year_Revenue, Total_Revenue, Free_Cash_Flow)" \
                              " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            val = (ticker_dict['ticker_id'], i, r['Basic EPS (FY)'], r['Basic EPS (TTM)'], r['EPS Diluted (FY)'],
                                   r['Net Income (FY)'], r['EBITDA (TTM)'], r['Gross Profit (MRQ)'], r['Gross Profit (FY)'],
                                   r['Last Year Revenue (FY)'], r['Total Revenue (FY)'], r['Free Cash Flow (TTM)'])
                            self.cur.execute(sql, val)
            self.con.commit()
        except:
            pass

    def insert_api_table(self, api, ids_list):
        """ insert or update Valuation table to mysql """
        df = api
        for i, r in df.iterrows():
            for ticker_dict in ids_list:
                if ticker_dict['Ticker'] == r[0]:
                    count_id = self.cur.execute("""SELECT ticker_id FROM API WHERE Ticker = %s""", (r[0]))
                    if count_id > 0:
                        self.cur.execute("""UPDATE API SET ticker_id = %s, Moving_Average_200_days_Exchange = %s, 
                             Address = %s, Description = %s WHERE Ticker = %s""", (ticker_dict['ticker_id'],
                            r[1], r[3], r[4][:255], r[0]))
                    else:
                        sql = "INSERT INTO API (ticker_id, Ticker, Moving_Average_200_days_Exchange, Address, Description) " \
                              "VALUES (%s, %s, %s, %s, %s) "
                        val = (ticker_dict['ticker_id'], r[0], r[1], r[3], r[4][:255])
                        self.cur.execute(sql, val)
        self.con.commit()


    def read_from_db(self, table):
        """ read and print from Mysql database by statement.
            params:       Columns(str),
                          Table(str),
                optional: Where(column(str),value(str)  """

        self.cur.execute("SELECT * FROM {};".format(table))
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
                          password='Kevin248',
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
    cur.execute(''' CREATE DATABASE IF NOT EXISTS Stock_Stats;''')
    cur.execute(''' USE Stock_Stats; ''')


def create_tables(con):
    """ create tables if don't exists. """

    cur = con.cursor()


    create_Sectors = '''
    CREATE TABLE IF NOT EXISTS `Sectors` (
    `sector_id` INT PRIMARY KEY AUTO_INCREMENT,
    `Sector` VARCHAR(255),
    `Market_Cap_B` FLOAT(10),
    `Change_Percent` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    `Vol_M` FLOAT(10),
    `Industries` INT,
    `Stocks` INT
    );'''

    cur.execute(create_Sectors)
    #############################

    create_Industry = '''
      CREATE TABLE IF NOT EXISTS `Industry` (
      `id` INT PRIMARY KEY AUTO_INCREMENT,
      `sector_id` INT,
      `Industry_Name` VARCHAR(255),
      `Mkt_Cap_B` FLOAT(10),
      `Change_Percent` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      `VOL_M` FLOAT(10),
      `Sector` VARCHAR(255),
      `Stocks` INT,
      FOREIGN KEY (`sector_id`) REFERENCES `Sectors` (`sector_id`)
      );'''

    cur.execute(create_Industry)

    #############################

    create_Main = ''' 
    CREATE TABLE IF NOT EXISTS Main (
    `ticker_id` INT PRIMARY KEY AUTO_INCREMENT, 
    `sector_id` INT,
    `Ticker` VARCHAR(255), 
    `Last` FLOAT(10), 
    `Change_Percent` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, 
    `Change` VARCHAR(255), 
    `Rating` VARCHAR(255), 
    `Volume` VARCHAR(255), 
    `Mkt_Cap` VARCHAR(255), 
    `Price_to_Earnings` VARCHAR(255), 
    `EPS` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    `Employees` VARCHAR(255), 
    `Sector` VARCHAR(255),
    FOREIGN KEY (`sector_id`) REFERENCES `Sectors` (`sector_id`)
    );'''
    cur.execute(create_Main)

    #############################

    create_Valuation = '''
    CREATE TABLE IF NOT EXISTS `Valuation` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `ticker_id` INT,
    `Ticker` VARCHAR(255),
    `Price_to_Revenue` DOUBLE,
    `Price_to_Book` DOUBLE,
    `Price_to_Sales` DOUBLE,
     FOREIGN KEY (`ticker_id`) REFERENCES `Main` (`ticker_id`)
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
          `Return_on_Invested_Capital` VARCHAR(255),
          `Revenue_per_Employee` VARCHAR(255),
           FOREIGN KEY (`ticker_id`) REFERENCES `Main` (`ticker_id`)
        );
        '''
    cur.execute(create_Metrics)

    #############################

    create_Balance_Sheet = '''
          CREATE TABLE IF NOT EXISTS `Balance_Sheet` (
          `id` INT PRIMARY KEY AUTO_INCREMENT,
          `ticker_id` INT,
          `Ticker` VARCHAR(255),
          `Quick_Ratio` VARCHAR(255),
          `Current_Ratio` VARCHAR(255),
          `Debt_to_Equity` VARCHAR(255),
          `Net_Debt` VARCHAR(255),
          `Total_Debt` VARCHAR(255),
          `Total_Assets` VARCHAR(255),
           FOREIGN KEY (`ticker_id`) REFERENCES `Main` (`ticker_id`)
        );
        '''
    cur.execute(create_Balance_Sheet)

    #############################

    create_Price_History = '''
    CREATE TABLE IF NOT EXISTS `Price_History` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `ticker_id` INT,
    `Ticker` VARCHAR(255),
    `1_Year_beta` VARCHAR(255),
    `52_Week_High` VARCHAR(255),
    `52_Week_Low` VARCHAR(255),
     FOREIGN KEY (`ticker_id`) REFERENCES `Main` (`ticker_id`)
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
     FOREIGN KEY (`ticker_id`) REFERENCES `Main` (`ticker_id`)
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
           FOREIGN KEY (`ticker_id`) REFERENCES `Main` (`ticker_id`)
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
           FOREIGN KEY (`ticker_id`) REFERENCES `Main` (`ticker_id`)
        );
        '''
    cur.execute(create_Income)

    create_api = '''
              CREATE TABLE IF NOT EXISTS `API` (
              `id` INT PRIMARY KEY AUTO_INCREMENT,
              `ticker_id` INT,
              `Ticker` VARCHAR(255),
              `Moving_Average_200_days_Exchange` VARCHAR(255),
              `Address` VARCHAR(255),
              `Description` VARCHAR(255)
            );
            '''
    cur.execute(create_api)

    con.commit()


def main():

    top_sectors = SectorScrapper.SectorScrapper(URL_SECTOR).summarizer()
    top_industries = IndustryScrapper.IndustryScrapper(URL_INDUSTRY).summarizer()
    top_market = TopMarketScrapper.TopMarketScrapper(URL).summarizer()

    db = Database()
    # db.insert_all_to_mysql(top_market, top_industries, top_sectors)

    db.close_connect_db()
    print("Done. ")


if __name__ == "__main__":
    main()
