## Data Mining Project
### By Kevin Daniels, Itamar Meimon  & Tal Toledano.
#### This "Web Scraping Project" currently includes the first two checkpoints required for the ITC data science group project assignment. The purpose is to retrieve data from a selected website and transfer it for storage in a separate database designed by the team.  

### The website of choice: https://www.tradingview.com/
Containing real time information about the U.S stock market.

	- The MainScrapper.py file contains a class StockScrapper.py which creates a dataframe and dictionary from the data by integrating several functions.

	- The other three classes ending in Scrapper.py (TopMarket, Industry, and Sector) are each responsible to collect their respective data.

	- The stock_parser function can receive user inputs and print relevant output following queries. 

	- For each individual parsed stock tables were split into seven columns and grouped by category.

To run the code the user will use the CLI to select the following information from the main scrappers. This will be done exclusively through MainScrapper.py, which will retrieve the information from each individual scrapper, and display the final result depending on what the user wishes to display: 

	- Select what table with its corresponding information will be displayed. The user can choose between:
		- Industry scrapper.
		- Sector scrapper.
		- All (Includes industry, sector and stocks scrapper)

	- Select from which row to scrap.

	- Select up to which row to scrap.

The database folder contains a Database.py which creates a database with relevant tables in SQL.

	- Following the scrapping in the classes, CSV files are created.
	- Database.py converts the generated CSV's to databases first by creating the tables and afterwards by inserting the data in them.
