# U.S. Stock Market Data Scraper and Analyzer

This project retrieves real-time and historical data about the U.S. stock market from [TradingView](https://www.tradingview.com/) and supplementary financial details via an API from [Alpha Vantage](https://www.alphavantage.co/). The collected data is then processed and stored in a MySQL database and as CSV backup files.

## Visuals

[To be added by user: Please consider adding a screenshot of the data, a chart generated from the output, or an image related to the tradingview.com website if appropriate and license permits. Unfortunately, an automated search for a suitable, rights-free image was not successful.]

## Prerequisites

Before running the project, ensure you have the following set up:

1.  **Python:** Python 3.x is required.
2.  **Libraries:** Install all necessary Python libraries using pip and the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```
3.  **MySQL Server:**
    *   A MySQL server must be running.
    *   The application connects to MySQL on `localhost` with the username `root` and password `password` by default.
    *   These credentials and other database configurations might be defined in `config.py` or directly within `Database/Database.py`. You may need to update these to match your MySQL setup.
    *   The script will automatically create the `Stock_Stats` database and the required tables if they do not already exist.
4.  **WebDriver:**
    *   This project uses Selenium for web scraping. Ensure you have the correct `chromedriver` for your operating system (Linux or macOS) and that its path is correctly configured if necessary, or that it's in your system's PATH. The repository includes `chromedriver_Linux` and `chromedriver_Mac`.

## How to Run

The main entry point for the application is `MainScrapper.py`. You can run it from the command line with different parameters to control the scope of data collection:

### 1. Concise Mode
Collects a summary of the top 100 stocks, a summary of all industries, and a summary of all sectors. This is the quickest mode.
```bash
python MainScrapper.py concise
```

### 2. Expanded Mode
Gathers detailed financial information for each of the top 100 stocks, including data from the Alpha Vantage API. This mode takes significantly longer than the concise mode.
```bash
python MainScrapper.py expanded
```

### 3. Expanded Mode for a Specific Ticker
Retrieves the same detailed information as the expanded mode but only for a single, user-specified stock ticker (providing 41 columns of data).
```bash
python MainScrapper.py expanded -ticker_to_scrap YOUR_TICKER_SYMBOL
```
For example, to get data for Tesla (TSLA):
```bash
python MainScrapper.py expanded -ticker_to_scrap TSLA
```

## Output

The scraped and processed data includes:
*   Stock market summaries (top stocks, industries, sectors).
*   Detailed financial information for individual stocks (e.g., last price, change percentage, P/E ratio, EPS, market cap, volume, financial ratios, balance sheet data, dividend info, etc.).
*   Additional company information like address and business description from API sources.

This data is stored in two main ways:
1.  **CSV Files:** Backup CSV files are created in the `Database/` directory (e.g., `Stock Info.csv`, `Industry Info.csv`, `Sector Info.csv`).
2.  **MySQL Database:** The primary storage is a MySQL database named `Stock_Stats`. The `Database/Database.py` script handles the creation of tables and insertion of data.

## Project Structure

*   `MainScrapper.py`: The main script to run the data scraping process. It parses command-line arguments and coordinates the actions of the various scrapper classes.
*   `Classes/`: This directory contains the Python classes responsible for scraping specific types of data:
    *   `TopMarketScrapper.py`: Scrapes data for the top market stocks.
    *   `StockScrapper.py`: Scrapes detailed financial data for individual stocks.
    *   `IndustryScrapper.py`: Scrapes data about different market industries.
    *   `SectorScrapper.py`: Scrapes data about different market sectors.
    *   `API_Scrapper.py`: Fetches additional data from external APIs (e.g., Alpha Vantage).
*   `Database/Database.py`: Manages all database operations, including connecting to MySQL, creating the database schema (tables), and inserting the scraped data.
*   `Database/`: This directory also holds the CSV backup files.
*   `requirements.txt`: A list of all Python dependencies required for the project.
*   `config.py`: (Likely) Contains configuration settings such as website URLs, API keys (if any), and database connection parameters. **Ensure you check and update this file with your details if necessary.**
*   `chromedriver_Linux`, `chromedriver_Mac`: WebDriver executables for Selenium to control Google Chrome on Linux and macOS respectively.
*   `Stocks.sql`: Potentially an SQL dump or schema definition, though `Database.py` handles schema creation dynamically.
*   `ReadMe.md`: This file.

---

*Note: The `stock_parser` function mentioned in the original README, which allowed user inputs for queries, seems to be integrated into or superseded by the CLI arguments of `MainScrapper.py`.*
*The original README also mentioned that "For each individual parsed stock tables were split into seven columns and grouped by category." This level of detail might be more suitable for code comments or more in-depth documentation if needed.*
