CREATE TABLE `Main` (
  `Ticker` varchar(255) PRIMARY KEY AUTO_INCREMENT,
  `Name` varchar(255),
  `Last` int,
  `Change_Percent` int,
  `Rating` varchar(255),
  `Volume` int,
  `Mkt_Cap` int,
  `Price_to_Earnings` int,
  `Employees` int,
  `Sector` varchar(255)
);

CREATE TABLE `Industry` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `Name` varchar(255),
  `Mkt_Cap` int,
  `Change_Percent` int,
  `Vol` int,
  `Sector` varchar(255)
);

CREATE TABLE `Sectors` (
  `Id` int PRIMARY KEY AUTO_INCREMENT,
  `Name` varchar(255),
  `Mkt_Cap` int,
  `Change_Percent` int,
  `Vol` int
);

CREATE TABLE `Valuation` (
  `Ticker` varchar(255) PRIMARY KEY AUTO_INCREMENT,
  `Market_Capitalization` int,
  `Enterprise_Value` int,
  `Enterprise_Value_EBITDA` int,
  `Total_Shares_Outstanding` int,
  `Number_Employees` int,
  `Number_Shareholders` int,
  `Price_to_Earnings` int,
  `Price_to_Revenue` int,
  `Price_Book` int,
  `Price_Sales` int
);

CREATE TABLE `Metrics` (
  `Ticker` varchar(255) PRIMARY KEY,
  `Return_on_Assets` int,
  `Return_on_Equity` int,
  `Return_on_Invested_Capital` int,
  `Revenue_per_Employee` int
);

CREATE TABLE `Balance_Sheet` (
  `Ticker` varchar(255) PRIMARY KEY,
  `Quick_Ratio` int,
  `Current_Ratio` int,
  `Debt_to_Equity` int,
  `Net_Debt` int,
  `Total_Debt` int,
  `Total_Assets` int
);

CREATE TABLE `Price_History` (
  `Ticker` varchar(255) PRIMARY KEY,
  `Average_Volume_10d` int,
  `1_Year_beta` int,
  `52_Week_High` int,
  `52_Week_Low` int
);

CREATE TABLE `Dividends` (
  `Ticker` varchar(255) PRIMARY KEY,
  `Dividends_Paid` int,
  `Dividends_Yield` int,
  `Dividends_per_Share` int
);

CREATE TABLE `Margins` (
  `Ticker` varchar(255) PRIMARY KEY,
  `Net_Margin` int,
  `Gross_Margin` int,
  `Operating_Margin` int,
  `Pretax_Margin` int
);

CREATE TABLE `Income` (
  `Ticker` varchar(255) PRIMARY KEY,
  `Basic_EPS_FY` int,
  `Basic_EPS_TTM` int,
  `EPS_Diluted` int,
  `Net_Income` int,
  `EBITDA` int,
  `Gross_Profit_MRQ` int,
  `Gross_Profit_FY` int,
  `Last_Year_Revenue` int,
  `Total_Revenue` int,
  `Free_Cash_Flow` int
);

ALTER TABLE `Main` ADD FOREIGN KEY (`Sector`) REFERENCES `Sectors` (`Id`);

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
