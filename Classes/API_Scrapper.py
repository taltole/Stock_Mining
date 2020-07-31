import requests
from config import *


def api_overview(ticker='AAPL'):
    """
    get data from api about specific stock.
    params: the stock name on trade view
    """

    querystring = {"symbol": ticker, "function": "OVERVIEW"}

    url = f'https://www.alphavantage.co/query?function={querystring["function"]}&symbol=' \
          f'{querystring["symbol"]}&apikey={API_KEY}'

    headers = {'x-rapidapi-host': "alpha-vantage.p.rapidapi.com",
               'x-rapidapi-key': "712a7be151msh49c200d2095f135p1aeae2jsn864918dad2ed"}

    response = requests.request("GET", url, headers=headers)
    json = response.json()

    try:
        data = {'Ticker': [ticker], 'Moving Average (200 days)': [api_sma(ticker)[-1]], 'Exchange': [json['Exchange']],
                'Address': [json['Address']], 'Description': [json['Description']]}
        df = pd.DataFrame(data)

    except Exception as ERR:
        df = [ticker, None, None, None, None]
        print(ERR)

    return df


def api_sma(ticker):
    """
    added additional data (moving average) from api about specific stock.
    params: the stock name on trade view
    """

    querystring = {"symbol": ticker, "function": "SMA", "interval": "daily", "time_period": "200",
                   "series_type": "close"}

    url = f'https://www.alphavantage.co/query?function={querystring["function"]}&symbol=' \
          f'{querystring["symbol"]}&interval='f'{querystring["interval"]}&time_period=' \
          f'{querystring["time_period"]}&series_type='f'{querystring["series_type"]}&apikey={API_KEY}'

    headers = {'x-rapidapi-host': "alpha-vantage.p.rapidapi.com",
               'x-rapidapi-key': "712a7be151msh49c200d2095f135p1aeae2jsn864918dad2ed"}

    response = requests.request("GET", url, headers=headers)
    json = response.json()
    lr = json['Meta Data']['3: Last Refreshed']
    sma = json["Technical Analysis: SMA"][lr]["SMA"]

    try:
        df = [ticker, lr, sma]
    except Exception as ERR:
        print(f'{ticker}: Failed')
        df = [ticker, None, None, None]

    return df


# ############################# ############################# ############################# ############################

# todo api to db each function for their right place on db.py


def insert_data_from_api(self, api_df):
    """ from api, insert data to mysql
    params: api_df with a general summary per user stock input
    """

    # add data to Image table
    sql = """INSERT IGNORE INTO InfoAPI
                (ticker_id, 'Ticker', 'Moving Average (200 days)', 'Exchange', 'Address', 'Description')
                VALUES (%s, %s, %s, %s, %s, %s)"""
    val = (None, api_df['Ticker'], api_df['Moving Average (200 days)'], api_df['Exchange'], api_df['Address'],
           api_df['Description'])
    self.cur.execute(sql, val)
    self.con.commit()


    create_InfoAPI = '''
            CREATE TABLE IF NOT EXISTS `InfoAPI` (
              `ticker_id` INT PRIMARY KEY AUTO_INCREMENT,
              `Ticker` VARCHAR(255),
              `Moving Average (200 days)` DOUBLE,
              `Exchange` VARCHAR(255),
              `Address` VARCHAR(255),
              `Description` VARCHAR(255)
            );
    '''
    cur.execute(create_InfoAPI)
    #############################


if __name__ == '__main__':
    df = api_overview()
    print(df)
    driver.close()
