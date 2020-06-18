import os
import sys
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import random

chorme_options = Options()
chorme_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chorme_options.add_argument("--disable-gpu")
chorme_options.add_argument("--headless")
chorme_options.add_argument("--no-sandbox")

# constant
DRIVER = os.path.join(os.getcwd(), 'chromedriver')
DELAY = random.randint(1, 5)
driver = webdriver.Chrome(DRIVER)
URL = "https://www.tradingview.com/symbols/NYSE-MO/"


class Table:
    def __init__(self, driver):
        """
        Instantiates the driver.
        """
        self.driver = driver

    def financial_titles(self):
        column_info = []
        financial_titles = self.driver.find_elements_by_xpath("//div[contains(@class,'tv-widget-fundamentals__item')]"
                                                              "//div[contains(@class,'tv-widget-fundamentals__title')]")
        for data in financial_titles:
            column_info.append(str(data.text).split('\n')[0])
        return column_info

    def rating_elements(self):
        elements = self.driver.find_elements_by_xpath(
            "//div[contains(@class,'tv-widget-fundamentals__item')]//div[contains(@class,'tv-widget-fundamentals__row')"
            "]//span[contains(@class, 'tv-widget-fundamentals__label apply-overflow-tooltip')]")
        element_info = []
        for element in elements:
            element_info.append(str(element.text).split('\n')[0])
        return element_info

    def rating_values(self):
        values = self.driver.find_elements_by_xpath(
            "//div[contains(@class,'tv-widget-fundamentals__item')]//div[contains(@class,'tv-widget-fundamentals__row')"
            "]//span[contains(@class, 'tv-widget-fundamentals__value apply-overflow-tooltip')]")
        value_info = []
        for value in values:
            value_info.append(str(value.text).split('\n')[0])
        return value_info

    def financial_table(self):
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
    table = Table(driver)
    driver.get(URL)
    print(table.financial_table())
    driver.close()


if __name__ == '__main__':
    main()
