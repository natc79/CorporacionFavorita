# CorporacionFavorita

Forecasting Corporacion Favorita product sales.  Exploratory forecasting of sales for ~4000 products across 54 grocery stores.  This code was developed in relation to Kaggle competition.

## Code Files

### masterCorporacionFavorita.R

Calls the relevant data files to process and forecast store and product sales.

### insertFiles-v2.R

This creates relevant data files that are intended to be used in forecasting.  It first creates a file of daily-store level data that contains:

1. Indicators of whether the day is a holiday at the city, state, and country level
2. Oil prices
3. Whether a day is a pay day

It processes the daily unit sales data for each product and store between 2013-2017 in chunks from train.csv (containing ~125 million observations) file into a SQL database by year for more rapid querying and converting the onpromotion variable into a 0/1 variable for reduced storage requirements.

### forecast_store_transactions.R

Trying to forecast results for ~4000 products for 54 stores requires significant computational power and could consume quite a bit of time.  pyspark potentially could be helpful in generating the computations.  However, while daily product level transactions might generate greater precision for the products I initially have simplified the forecast problem by instead forecasting store-level transaction data for the 54 stores.  The initial hypothesis and something that was seemingly confirmed by some initial examinations into the data is that there is greater variance in product purchasing across stores rather than within the store.  Moreover the relative shares of product purchases do not change extremely dramatically over time, but are more likely to vary in relation to total store transactions.

In this code an auto.arima model that automates the selection of moving average and lagged dependent variables with seasonal level effects that controls for holiday, event, pay day, and oil prices is used to forecast transactions two weeks into the future. It also graphs the predicted time series forecast for each store level model outputting them into a file.

### forecast_item_sales.R

This code computes average shares of product sales within the store over time and depending on whether the product was onpromotion.  It then applies these shares to the daily transaction data to come up with an estimated forecast of product level unit sales within a store. 
