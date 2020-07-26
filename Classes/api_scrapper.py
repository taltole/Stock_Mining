import requests
from Classes.config import *

def api_overview(ticker):
    """
    get data from api about specific stock.
    params: the stock name on trade view
    """
    api_code = 'IQGX1FT91GHD48FM'

    querystring = {"symbol": ticker, "function": "OVERVIEW"}

    url = f'https://www.alphavantage.co/query?function={querystring["function"]}&symbol=' \
          f'{querystring["symbol"]}&apikey={api_code}'

    headers = {'x-rapidapi-host': "alpha-vantage.p.rapidapi.com",
               'x-rapidapi-key': "712a7be151msh49c200d2095f135p1aeae2jsn864918dad2ed"}

    response = requests.request("GET", url, headers=headers)
    json = response.json()

    try:
        data = {'Ticker': [ticker + 'D'], 'Moving Average (200 days)': [api_sma(ticker)[-1]], 'Exchange': [json['Exchange']],
                'Address': [json['Address']], 'Description': [json['Description']]}
        df = pd.DataFrame(data)

    except Exception as ERR:
        df = [ticker + 'D', None, None, None, None]
        print(ERR)

    return df


def api_sma(ticker):
    """
    added additional data (moving average) from api about specific stock.
    params: the stock name on trade view
    """

    api_code = 'IQGX1FT91GHD48FM'
    querystring = {"symbol": ticker, "function": "SMA", "interval": "daily", "time_period": "200",
                   "series_type": "close"}

    url = f'https://www.alphavantage.co/query?function={querystring["function"]}&symbol=' \
          f'{querystring["symbol"]}&interval='f'{querystring["interval"]}&time_period=' \
          f'{querystring["time_period"]}&series_type='f'{querystring["series_type"]}&apikey={api_code}'

    headers = {'x-rapidapi-host': "alpha-vantage.p.rapidapi.com",
               'x-rapidapi-key': "712a7be151msh49c200d2095f135p1aeae2jsn864918dad2ed"}

    response = requests.request("GET", url, headers=headers)
    json = response.json()
    lr = json['Meta Data']['3: Last Refreshed']
    sma = json["Technical Analysis: SMA"][lr]["SMA"]

    try:
        df = [ticker + 'D', lr, sma]
    except Exception as ERR:
        print(f'{ticker}: Failed')
        df = [ticker + 'D', None, None, None]

    return df

if __name__ == '__main__':
    df = api_overview()
    print(df)
    driver.close()