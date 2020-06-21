
"""
Get stock analysis, stats and updates
"""
import os
# import random
# import pandas as pd
# from selenium import webdriver
from itertools import takewhile
#
from DataMining.DataMining.Classes.config import *

#
# URL = 'https://www.tradingview.com/markets/stocks-usa/sectorandindustry-industry/'
# DELAY = random.randint(1, 5)
# driver = webdriver.Chrome(os.path.join(os.getcwd(), 'chromedriver'))


class IndustryScrapper:

    def __init__(self, URL_INDUSTRY):
        self.URL_INDUSTRY = URL_INDUSTRY

    def industry_scrapper(self):
        """
        this func will look for kw for each site main_scraper returns
        :return: purchase links, prices, top choices.
        """
        driver.get(URL_INDUSTRY)

        # getting sectors concise info
        industry = []
        data_list = []
        industries = driver.find_elements_by_class_name('tv-data-table__tbody')

        data_list_long = [i.text for i in industries[1:]][0].split('\n')
        for j in data_list_long:
            data_list.append(''.join(takewhile(lambda x: not x.isdigit(), j)))

        temp_list = []
        for element in data_list_long:
            new_element = element.split()
            digit_found = False
            index = 0
            i = 0
            while i < len(new_element) and not digit_found:
                if new_element[i][0].isdigit() and not digit_found:
                    index = i
                    digit_found = True
                i += 1
            temp_list.append(new_element[index:])
        final_list = []
        for element in temp_list:
            if len(element) == 7:
                first = element[:4]
                second = ' '.join([element[4], element[5]])
                first.append(second)
                third = element[-1]
                first.append(third)
                final_list.append(first)
            else:
                final_list.append(element)

        data_len = len(data_list_long)
        for i in range(0, data_len):
            industry.append(data_list[i])

        return industry, final_list

    def summarizer(self):
        """
        sum info in data frame
        """
        driver.get(URL_INDUSTRY)
        industry, final_list = self.industry_scrapper()

        # get main page headers
        header_industry = driver.find_element_by_xpath('//*[@id="js-screener-container"]/div[3]/table/thead')
        header_industry = header_industry.text.replace('INDUSTRY\n\n128 matches', 'INDUSTRY').split('\n')

        # creating data frame
        df_industry = pd.DataFrame(index=industry, data=final_list, columns=header_industry[1:])

        return df_industry

    def to_csv(self):
        """
        get df and output to csv file
        """
        df_industry = self.summarizer()

        # create CSV file
        if not df_industry.empty:
            # if file does not exist write header
            file_name = 'Industry_info.csv'
            if not os.path.isfile(file_name):
                df_industry.to_csv(file_name, encoding='utf-8')
            else:  # else it exists so append without writing the header
                df_industry.to_csv(file_name, encoding='utf-8', mode='w', header=False)
