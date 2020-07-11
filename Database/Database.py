"""
Database class:
    handle the database.
    first, can take data after the web scraping and create database with relevant tables.
"""

# TODO: handle duplicate insert values.

import pymysql.cursors
from DataMining.Classes.config import *


class Database:
    def __init__(self):
        """ connect to database. if don't exists - create database and tables. """

        self.con, self.cur = setup_mysql_db()
        self.df = read_csv(CSV_FILE)

    def close_connect_db(self):
        """ close connection to Mysql database. """
        self.con.close()

    def insert_main_table(self):
        """ from CSV file, insert Countries table to mysql """

        df = self.df
        for i, r in df.iterrows():
            sql = "INSERT IGNORE INTO Main (Ticker, Name, Last, Change %, Rating, Volume, Mkt Cap, Price to Earnings," \
                  " Employees, Sector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (None, r['Ticker'], r['Name'], r['Last'], r['Change_Percent'], r['Rating'], r['Volume'], r['Mkt_Cap'],
                   r['Price_to_Earnings'], r['Employees'], r['Sector'])
            sql = "INSERT IGNORE INTO Industry (id, Name, Mkt_Cap, Change %, Vol, Sector)" \
                  " VALUES (%s, %s, %s, %s, %s, %s)"
            val = (None, r['id'], r['Name'], r['Mkt_Cap'], r['Change_Percent'], r['Vol'], r['Sector'])
            sql = "INSERT IGNORE INTO Sectors (id, Name, Mkt_Cap, Change %, Vol)" \
                  " VALUES (%s, %s, %s, %s, %s)"
            val = (None, r['id'], r['Name'], r['Mkt_Cap'], r['Change_Percent'], r['Vol'])
            sql = "INSERT IGNORE INTO Valuation (id, Ticker, Market Cap, Enterprise Value, Enterprise Value to EBITDA," \
                  "Total_Shares_Outstanding, Number of Employees, Number of Shareholders, Price to Earnings," \
                  "Price to Revenue, Price to Book, Price to Sales ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"\
            val = (None, r['id'], r['Ticker'], r['Mkt_Cap'], r['Enterprise_Value'], r['Enterprise_Value_EBITDA'],
                   r['Total_Shares_Outstanding'], r['Number_Employees'], r['Number_Shareholders'], r['Price_to_Earnings'],
                   r['Price_to_Revenue'], r['Price_Book'], r['Price_Sales'])
            sql = "INSERT IGNORE INTO Metrics (id, Ticker, Return on Assets, Return on Equity, Return on Invested Capital," \
                  "Revenue per Employee) VALUES (%s, %s, %s, %s, %s, %s)" \
            val = (None, r['id'], r['Ticker'], r['Return_on_Assets'], r['Return_on_Equity'], r['Return_on_Invested_Capital'],
                   r['Revenue_per_Employee'])
            sql = "INSERT IGNORE INTO Balance_Sheet (id, Ticker, Quick Ratio, Current Ratio, Debt to Equity, Net Debt" \
                  "Total Debt, Total Assets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (None, r['id'], r['Ticker'], r['Quick_Ratio'], r['Current_Ratio'], r['Debt_to_Equity'], r['Net_Debt'],
                   r['Total_Debt'], r['Total_Assets'])
            sql = "INSERT IGNORE INTO Price_History (id, Ticker, Average Volume (10 days), 1 Year beta, 52 week high," \
                  "52 week low) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (None, r['id'], r['Ticker'], r['Average_Volume_10d'], r['1_Year_beta'], r['52_Week_High'], r['52_Week_Low'])
            sql = "INSERT IGNORE INTO Dividends (id, Ticker, Dividends Paid, Dividends Yield, Dividends per Share)" \
                  " VALUES (%s, %s, %s, %s, %s)"
            val = (None, r['id'], r['Ticker'], r['Dividends_Paid'], r['Dividends_Yield'], r['Dividends_per_Share'])
            sql = "INSERT IGNORE INTO Margins (id, Ticker, Net Margin, Gross Margin, Operating Margin, Pretax Margin" \
                  " VALUES (%s, %s, %s, %s, %s, %s)"
            val = (None, r['id'], r['Ticker'], r['Net_Margin'], r['Gross_Margin'], r['Operating_Margin'], r['Pretax_Margin'])
            sql = "INSERT IGNORE INTO Income (id, Ticker, Basic EPS FY, Basic EPS TTM, EPS Diluted, Net Income, EBITDA," \
                  "Gross Profit MRQ, Gross Profit FY, Last Year Revenue, Total Revenue, Free Cash Flow" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (None, r['id'], r['Ticker'], r['Basic_EPS_FY'], r['Basic_EPS_TTM'], r['EPS_Diluted'], r['Net_Income'],
                   r['EBITDA'], r['Gross_Profit_MRQ'], r['Gross_Profit_FY'], r['Last_Year_Revenue'], r['Total_Revenue'],
                   r['Free_Cash_Flow'])



        self.cur.execute(sql, val)
        self.con.commit()
        # todo def_TABLE_table for all tables - DONE (SEE ABOVE)

    def read_from_db(self, columns, table, where=''):
        """ read and print from Mysql database by statement.
            params:       Columns(str),
                          Table(str),
                optional: Where(column(str),value(str)  """

        if where:
            self.cur.execute("SELECT {} FROM {} WHERE {}='{}'".format(columns, table, where[0], where[1]))
        else:
            self.cur.execute("SELECT {} FROM {} ".format(columns, table))

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

    con = pymysql.cursors.connect(
        host='localhost', user=MYSQL_USERNAME, use_pure=True, auth_plugin='mysql_native_password')

    # create if don't exists:
    create_database(con)
    create_tables(con)

    return con, con.cursor()


def create_database(con):
    """ create database if don't exists. """
    cur = con.cursor()
    cur.execute(''' ''')
    con.commit()


def create_tables(con):
    """ create tables if don't exists. """

    cur = con.cursor()

    create_Main = '''
          CREATE TABLE IF NOT EXISTS `Main` (
          "Ticker" varchar(255) [pk, increment]
          "Name" varchar(255)
          "Last" int
          "Change_Percent" float
          "Rating" varchar(255)
          "Volume" int
          "Mkt_Cap" int
          "Price_to_Earnings" int 
          "Employees" int
          "Sector" varchar(255) [ref: > Sectors.Id]
        );'''

    cur.execute(create_Main)


    #############################


    create_Industry = '''
          CREATE TABLE IF NOT EXISTS `Industry` (
          "Id" int [pk, increment]
          "Name" varchar
          "Mkt_Cap" int
          "change_Percent" int
          "Vol" int
          "Sector" varchar(255)
        );
        '''
    cur.execute(create_Industry)

    #############################

    create_Sectors = '''
          CREATE TABLE IF NOT EXISTS 'Sectors' (
          "id" double [pk, increment]
          "Name" varchar(255)
          "Mkt_Cap" float
          "Change_Percent" float
          "Vol" float
        );
        '''
    cur.execute(create_Sectors)

    #############################

    create_Valuation = '''
          CREATE TABLE IF NOT EXISTS 'Valuation' (
          "id" double [pk, increment]
          "Ticker" varchar(255)
          "Market_Capitalization" float
          "Enterprise_Value" float
          "Enterprise_Value_EBITDA" float
          "Total_Shares_Outstanding" float
          "Number_Employees" float
          "Number_Shareholders" float
          "Price_to_Earnings" float
          "Price_to_Revenue" float
          "Price_Book" float
          "Price_Sales" float
        );
        '''
    cur.execute(create_Valuation)

    #############################

    create_Metrics = '''
          CREATE TABLE IF NOT EXISTS 'Metrics' (
          "id" double [pk, increment]
          "Ticker" varchar(255)
          "Return_on_Assets" float
          "Return_on_Equity" float
          "Return_on_Invested_Capital" float
          "Revenue_per_Employee" float
        );
        '''
    cur.execute(create_Metrics)

    #############################

    create_Balance_Sheet = '''
          CREATE TABLE IF NOT EXISTS 'Balance_Sheet' (
          "id" double [pk, increment]
          "Ticker" varchar(255)
          "Quick_Ratio" float
          "Current_Ratio" float
          "Debt_to_Equity" float
          "Net_Debt" float
          "Total_Debt" float
          "Total_Assets" float
        );
        '''
    cur.execute(create_Balance_Sheet)

    #############################

    create_Price_History = '''
          CREATE TABLE IF NOT EXISTS 'Price_History' (
          "id" double [pk, increment]
          "Ticker" varchar(255)
          "Average_Volume_10d" float
          "1_Year_beta" float
          "52_Week_High" float
          "52_Week_Low" float
        );
        '''
    cur.execute(create_Price_History)

    #############################

    create_Dividends = '''
          CREATE TABLE IF NOT EXISTS 'Dividends' (
          "id" double [pk, increment]
          "Ticker" varchar(255)
          "Dividends_Paid" float
          "Dividends_Yield" float
          "Dividends_per_Share" float
        );
        '''
    cur.execute(create_Dividends)

    #############################

    create_Margins = '''
          CREATE TABLE IF NOT EXISTS 'Margins' (
          "id" double [pk, increment]
          "Ticker" varchar(255)
          "Net_Margin" float
          "Gross_Margin" float
          "Operating_Margin" float
          "Pretax_Margin" float
        );
        '''
    cur.execute(create_Margins)

    #############################

    create_Income = '''
          CREATE TABLE IF NOT EXISTS 'Income' (
          "id" double [pk, increment]
          "Ticker" varchar(255)
          "Basic_EPS_FY" float
          "Basic_EPS_TTM" float
          "EPS_Diluted" float
          "Net_Income" float
          "EBITDA" float
          "Gross_Profit_MRQ" float
          "Gross_Profit_FY" float
          "Last_Year_Revenue" float
          "Total_Revenue" float
          "Free_Cash_Flow" float

        );
        '''
    cur.execute(create_Margins)

    #############################

# todo add for all tables - DONE - See above. Main table still has [ref: > Sectors.Id] from before - to delete?


def main():
    db = Database()
    return


if __name__ == "__main__":
    main()
