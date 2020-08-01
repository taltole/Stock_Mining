"""
Get industry analysis, stats and updates
"""
from itertools import takewhile
from config import *
import time


class IndustryScrapper:

    def __init__(self, URL_INDUSTRY):
        self.URL_INDUSTRY = URL_INDUSTRY

    @classmethod
    def industry_scrapper(self):
        """
        this counter will look for kw for each site main_scraper returns
        :return: purchase links, prices, top choices.
        """
        driver.get(URL_INDUSTRY)

        # getting sectors concise info
        industry = []
        data_list = []
        industries = driver.find_elements_by_class_name('tv-data-table__tbody')

        data_list_long = [i.text for i in industries][0].split('\n')
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

    @classmethod
    def summarizer(self):
        """
        sum info in data frame
        """
        driver.get(URL_INDUSTRY)
        industry, final_list = self.industry_scrapper()

        [final_list[i].insert(0, industry[i][:]) for i in range(len(industry))]
        for ind, row in enumerate(final_list):
            if len(row) > 7:
                final_list[ind] = row[:7]

        # get main page headers
        header_industry = ['INDUSTRY', 'MKT CAP', 'DIV YIELD', 'CHG PERCENT', 'VOL', 'SECTOR', 'STOCKS']

        # creating data frame
        df_industry = pd.DataFrame(data=final_list, columns=header_industry)

        # Create CSV backup
        filename = 'Industry Info.csv'
        df_industry.to_csv(PATH_DB + filename, encoding='utf-8', mode='w', header=True)

        return df_industry

    def create_csv(self):
        """
        get df and output to csv file
        """
        df_industry = self.summarizer()

        # create CSV file
        if not df_industry.empty:
            # if file does not exist write header
            filename = 'Industry info.csv'
            if not os.path.isfile(PATH_DB+filename):
                df_industry.to_csv(PATH_DB+filename, encoding='utf-8')
            else:  # else it exists so append without writing the header
                df_industry.to_csv(PATH_DB+filename, encoding='utf-8', mode='w', header=True)

