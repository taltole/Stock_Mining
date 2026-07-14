<div align="center">

# Stock Mining

**Data mining and analysis pipeline for stock market data**<br>
Web scraping · Financial feature engineering · EDA · Trend analysis

[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=flat-square&logo=selenium&logoColor=white)](https://selenium.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)](LICENSE)

</div>

---

## Overview

End-to-end data mining project on stock market data — from raw scraping through feature engineering, EDA, and trend modelling.

**Goals:**
- Collect clean historical stock data from multiple sources
- Engineer financial indicators from raw OHLCV data
- Identify patterns and correlations across sectors and timeframes

---

## Pipeline

```
Data Sources (Yahoo Finance / web scraping)
        |
[ Data Collection ]
  Selenium scraper with anti-bot handling
  pandas-datareader / yfinance API
  multi-ticker batch download
        |
[ Feature Engineering ]
  Moving averages (SMA, EMA)
  RSI, MACD, Bollinger Bands
  Volume-weighted metrics
        |
[ EDA & Analysis ]
  Correlation heatmaps
  Seasonality patterns
  Volatility analysis
        |
[ Visualisation ]
  Candlestick charts
  Rolling statistics plots
```

---

## Features Engineered

| Feature | Description |
|---------|-------------|
| SMA(20), SMA(50) | Simple moving averages |
| EMA(12), EMA(26) | Exponential moving averages |
| MACD | Trend momentum indicator |
| RSI(14) | Relative strength index |
| Bollinger Bands | Volatility channel |
| Daily Return | % change close-to-close |

---

## Quickstart

```bash
pip install -r requirements.txt
python main.py
```

Configure tickers and date range in config.py:

```python
TICKERS   = ["AAPL", "MSFT", "GOOGL", "AMZN"]
START     = "2018-01-01"
END       = "2023-12-31"
```

---

## Design Decisions

**Why Selenium?** Some financial data sources use dynamic JavaScript rendering that defeats simple requests + BeautifulSoup scraping.

**Why feature engineering over raw prices?** Raw price series are non-stationary. Engineered indicators (RSI, MACD, Bollinger Bands) capture momentum and volatility patterns that are more predictive.

---

## Author

**Tal A. Toledano** — Data Scientist & ML Engineer
[GitHub](https://github.com/taltole) · taltole@me.com

---

*Part of a broader CV/ML portfolio:*
[Revenue_TimeSeries](https://github.com/taltole/Revenue_TimeSeries) · [Scrap_Movies](https://github.com/taltole/Scrap_Movies)
