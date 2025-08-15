# financial-python-qlik-sse
This repository provides some Finance-Focused functions using a server-side extension (SSE) for Qlik Sense built using Python.

# Prerequisites
Python 3.7+ installed on your server

# Required Python packages
```bash
pip install grpcio grpcio-tools pandas numpy statsmodels scipy
```
# Generate gRPC Protocol Files
Download the QlikSense SSE protocol files from:
https://github.com/qlik-oss/server-side-extension/tree/master

Run the following command to generate files:
```bash
python -m grpc_tools.protoc -I proto --proto_path=. --python_out=. --grpc_python_out=. ServerSideExtension.proto
```
This creates:

<img width="525" height="320" alt="gRPC Protocol Files" src="https://github.com/user-attachments/assets/3fa7a5c4-af06-4bc7-a1fb-d15e810a69da" />

# Start the SSE server
```bash
python ssePyToolsProfit.py
```
Verify port: Ensure port that you goes to use is not blocked by firewall

<img width="652" height="249" alt="Python SSE Server" src="https://github.com/user-attachments/assets/51e7ff39-ae02-43d4-934a-59d098f4d55d" />

# Qlik Sense
Set up a new analytic connection

<img width="752" height="349" alt="Qlik Sense Analytic Connection" src="https://github.com/user-attachments/assets/d9e069e1-543c-4089-9bb9-33243afe22b9" />

Restart Qlik Sense Engine Service

<img width="500" height="149" alt="Qlik Sense Services" src="https://github.com/user-attachments/assets/2066cab6-3242-49c9-a9a2-5d8ee8b079a7" />

In your app verify your regional configurations, for example money o number formats:

```Qlik Script
SET DecimalSep='.';
SET ThousandSep=',';
```

In this example we can determinate some financial analysis using three functions by ssePyToolsProfit.

```Qlik Script
FinancialData:
LOAD
    year as Year,
    month as Month,
    Date(MakeDate(year, month), 'MMM') as MonthName,
    idProduct&' - '&nameProduct as Product,
    backOrder,
    unitsSold,
    totalSales,
    fixedCosts,
    variableCosts,
    fixedCosts + variableCosts as totalCosts,
    totalSales - (fixedCosts + variableCosts) as Profit,
    (totalSales - (fixedCosts + variableCosts)) / totalSales as profitMargin,
    totalSales / unitsSold as pricePerUnit,
    variableCosts / unitsSold as variableCostPerUnit    
FROM [lib://Qvds/dev/files/example.csv]
(txt, codepage is 28591, embedded labels, delimiter is ',', msq);

MasterCalendar:
LOAD 
    Month,
    MonthName,
    Year,
    Date(MakeDate(Year, Month, 1)) as MonthYear,
    'Q' & Ceil(Month/3) as Quarter,
    If(Month <= 6, 'H1', 'H2') as HalfYear,
    Year & ' ' & MonthName as MonthYearLabel
RESIDENT FinancialData;
```

All three functions are Aggregate type, so their effect is much better appreciated in KPIs. The analysis included are: margin volatility, break-even and price elasticity.

<img width="1024" height="768" alt="Result of the function" src="https://github.com/user-attachments/assets/090cfbcd-6235-48e6-a3b9-eb13fb827a0e" />

