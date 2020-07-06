CREATE TABLE `Main` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Ticker` varchar(255),
  `Name` varchar(255),
  `Last` float,
  `Change_Percent` float,
  `Rating` varchar(255),
  `Volume` float,
  `Mkt_Cap` float,
  `Price_to_Earnings` float,
  `Employees` float,
  `Sector` varchar(255)
);

CREATE TABLE `Industry` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Name` varchar(255),
  `Mkt_Cap` float,
  `Change_Percent` float,
  `Vol` float,
  `Sector` varchar(255)
);

CREATE TABLE `Sectors` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Name` varchar(255),
  `Mkt_Cap` float,
  `Change_Percent` float,
  `Vol` float
);

CREATE TABLE `Valuation` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Ticker` varchar(255),
  `Market_Capitalization` float,
  `Enterprise_Value` float,
  `Enterprise_Value_EBITDA` float,
  `Total_Shares_Outstanding` float,
  `Number_Employees` float,
  `Number_Shareholders` float,
  `Price_to_Earnings` float,
  `Price_to_Revenue` float,
  `Price_Book` float,
  `Price_Sales` float
);

CREATE TABLE `Metrics` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Ticker` varchar(255),
  `Return_on_Assets` float,
  `Return_on_Equity` float,
  `Return_on_Invested_Capital` float,
  `Revenue_per_Employee` float
);

CREATE TABLE `Balance_Sheet` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Ticker` varchar(255),
  `Quick_Ratio` float,
  `Current_Ratio` float,
  `Debt_to_Equity` float,
  `Net_Debt` float,
  `Total_Debt` float,
  `Total_Assets` float
);

CREATE TABLE `Price_History` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Ticker` varchar(255),
  `Average_Volume_10d` float,
  `1_Year_beta` float,
  `52_Week_High` float,
  `52_Week_Low` float
);

CREATE TABLE `Dividends` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Ticker` varchar(255),
  `Dividends_Paid` float,
  `Dividends_Yield` float,
  `Dividends_per_Share` float
);

CREATE TABLE `Margins` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Ticker` varchar(255),
  `Net_Margin` float,
  `Gross_Margin` float,
  `Operating_Margin` float,
  `Pretax_Margin` float
);

CREATE TABLE `Income` (
  `id` double PRIMARY KEY AUTO_INCREMENT,
  `Ticker` varchar(255),
  `Basic_EPS_FY` float,
  `Basic_EPS_TTM` float,
  `EPS_Diluted` float,
  `Net_Income` float,
  `EBITDA` float,
  `Gross_Profit_MRQ` float,
  `Gross_Profit_FY` float,
  `Last_Year_Revenue` float,
  `Total_Revenue` float,
  `Free_Cash_Flow` float
);

ALTER TABLE `Main` ADD FOREIGN KEY (`Sector`) REFERENCES `Sectors` (`id`);

ALTER TABLE `Industry` ADD FOREIGN KEY (`Sector`) REFERENCES `Sectors` (`id`);

ALTER TABLE `Industry` ADD FOREIGN KEY (`Sector`) REFERENCES `Sectors` (`Name`);

ALTER TABLE `Valuation` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`Ticker`);

ALTER TABLE `Valuation` ADD FOREIGN KEY (`Market_Capitalization`) REFERENCES `Main` (`Mkt_Cap`);

ALTER TABLE `Valuation` ADD FOREIGN KEY (`Price_to_Earnings`) REFERENCES `Main` (`Price_to_Earnings`);

ALTER TABLE `Metrics` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`Ticker`);

ALTER TABLE `Balance_Sheet` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`Ticker`);

ALTER TABLE `Price_History` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`Ticker`);

ALTER TABLE `Dividends` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`Ticker`);

ALTER TABLE `Margins` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`Ticker`);

ALTER TABLE `Income` ADD FOREIGN KEY (`Ticker`) REFERENCES `Main` (`Ticker`);
