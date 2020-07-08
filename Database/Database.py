"""
Database class:
    handle the database.
    first, can take data after the web scraping and create database with relevant tables.
"""

# TODO: handle duplicate insert values.

import pymysql.cursors
from DataMining.DataMining.Classes.config import *


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
            sql = "INSERT IGNORE INTO Main (Ticker, Name, Last, Change %, Rating, Volume, Mkt_Cap, Price_to_Earnings," \
                  " Employees, Sector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (None, r['Ticker'], r['Name'], r['Last'], r['Change_Percent'], r['Rating'], r['Volume'], r['Mkt_Cap'],
                   r['Price_to_Earnings'], r['Employees'], r['Sector'])
            self.cur.execute(sql, val)
        self.con.commit()
        # todo def_TABLE_table for all tables

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
          "Change_precent" float
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
# todo add for all tables


def main():
    db = Database()
    return


if __name__ == "__main__":
    main()
