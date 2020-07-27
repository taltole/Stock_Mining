## Data Mining Project
### By Kevin Daniels, Itamar Meimon  & Tal Toledano.
#### This "Web Scraping Project" currently includes the first three checkpoints required for the ITC data science group project assignment. The purpose is to retrieve data from a selected website and transfer it for storage in a separate database designed by the team. Additional relevant information is extracted from another website via an API. 

### The website of choice: https://www.tradingview.com/
Containing real time information about the U.S stock market.

	- The MainScrapper.py file contains a class StockScrapper.py which creates a dataframe and dictionary from the data by integrating several functions.

	- The other three classes ending in Scrapper.py (TopMarket, Industry, and Sector) are each responsible to collect their respective data.

	- The stock_parser function can receive user inputs and print relevant output following queries. 

	- For each individual parsed stock tables were split into seven columns and grouped by category.

To run the code the user will use the CLI to select the following information from the main scrappers. This will be done exclusively through MainScrapper.py, which will retrieve the information from each individual scrapper, and display the final result depending on what the user wishes to display: 

	- The user can choose between the following parameters:
		- concise
		- expanded 
		- expanded -ticker_to_scrap (+ chosen ticker)

	- With the concise parameter, the user gets the summary of the top 100 stocks, and summary on all industries, and a summary on all sectors. 
	  Example to run from CLI: python MainScrapper.py concise 

	- With the expanded parameter, the user gets much more details on each of the top 100 stocks with full financial information on each stock.
	  Takes much more time than concise. Included data extracted via an API from the following website: https://www.alphavantage.co/
	  Example to run from CLI: python MainScrapper.py expanded
	
	- With the expanded -ticker_to_scrap parameter, the user gets all the information in the expanded parameter but only on the specific chosen stock (41 columns).
	  Some of the columns such as Address, Exchange, and Moving Average (200 days) were extracted via an API from the following website: https://www.alphavantage.co/
	  Example with Tesla stock to run from CLI: python MainScrapper.py expanded -ticker_to_scrap TSLA   

The database folder contains a Database.py which creates a database with relevant tables in SQL.

	- Following the scrapping, each class creates a Pandas dataframe (as well as a backup CSV file)
	- Database.py converts the generated dataframes to databases first by creating the tables and afterwards by inserting the data in them.

	* When running Database.py, please make sure to change the password and any other relevant setup information for pymysql (in the setup_mysql_db() function)


