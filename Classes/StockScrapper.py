"""
This file contains contains a class which creates a table of rating elements and its respective values for a given url.
"""
from Classes import TopMarketScrapper
from Classes.config import *


class StockScrapper:
    """
    This class integrates several functions to retrieve the information of rating elements given a certain stock and
    creates a dictionary with all the information for each one of them.
    """
    def __init__(self, driver):
        """
        Constructs the driver for the class.
        """
        self.driver = driver

    def financial_titles(self):
        """
        This function retrieves the headers of the different groups of rating elements.
        :return: list
        """
        column_info = []
        financial_titles = self.driver.find_elements_by_xpath("//div[contains(@class,'tv-widget-fundamentals__item')]"
                                                              "//div[contains(@class,'tv-widget-fundamentals__title')]")
        for data in financial_titles:
            column_info.append(str(data.text).split('\n')[0])
        return column_info

    def rating_elements(self):
        """
        List of all the rating elements for each stock.
        :return: list
        """
        elements = self.driver.find_elements_by_xpath(
            "//div[contains(@class,'tv-widget-fundamentals__item')]//div[contains(@class,'tv-widget-fundamentals__row')"
            "]//span[contains(@class, 'tv-widget-fundamentals__label apply-overflow-tooltip')]")
        element_info = []
        for element in elements:
            element_info.append(str(element.text).split('\n')[0])
        return element_info

    def rating_values(self):
        """
        List of rating values for each rating element. It has the same size as
        rating_elements and the indexes correspond to the same order.
        :return: list
        """
        values = self.driver.find_elements_by_xpath(
            "//div[contains(@class,'tv-widget-fundamentals__item')]//div[contains(@class,'tv-widget-fundamentals__row')"
            "]//span[contains(@class, 'tv-widget-fundamentals__value apply-overflow-tooltip')]")
        value_info = []
        for value in values:
            value_info.append(str(value.text).split('\n')[0])
        return value_info

    def financial_table(self):
        """
        Creates a dictionary representing a financial table for each stock.
        :return: dict
        """
        column_titles = self.financial_titles()
        rating_elements = self.rating_elements()
        rating_values = self.rating_values()
        table = {}
        table[column_titles[0]] = []
        table[column_titles[1]] = []
        table[column_titles[2]] = []
        table[column_titles[3]] = []
        table[column_titles[4]] = []
        table[column_titles[5]] = []
        for row in range(len(rating_elements)):
            if row < 10:
                table[column_titles[0]].append([rating_elements[row], rating_values[row]])
            elif 10 <= row < 14:
                table[column_titles[1]].append([rating_elements[row], rating_values[row]])
            elif 14 <= row < 20:
                table[column_titles[2]].append([rating_elements[row], rating_values[row]])
            elif 20 <= row < 27:
                table[column_titles[3]].append([rating_elements[row], rating_values[row]])
            elif 27 <= row < 31:
                table[column_titles[4]].append([rating_elements[row], rating_values[row]])
            elif 31 <= row < 41:
                table[column_titles[5]].append([rating_elements[row], rating_values[row]])
        return table


def main():
    """
    Given a URL of the stock market and the url's for each stock imported from TopMarketScrapper.py, creates a DataFrame
    and financial table of all the stocks.
    :return: DF and dict
    """
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
        table = StockScrapper(driver)
        driver.get(url)
        list_elements[0].append(table.rating_elements())
        list_values.append(table.rating_values())
        stock_table[list_stocks[index_stock]] = table.financial_table()
        driver.close()
        index_stock += 1
    df_table = pd.DataFrame(list_values, index=list_stocks, columns=list_elements[0][0])
    df_table.to_csv(path_or_buf=PATH_DB+filename)
    print(stock_table)
    print(df_table)


if __name__ == '__main__':
    main()
