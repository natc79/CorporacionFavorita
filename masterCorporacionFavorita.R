# Master Corporacion Favorita
# This calls all of the relevant files
# -----------------------------------------------------------------------



# 1.  Data Insertion ----------------------------------------------------------
# This file creates:
# a) transactions_stores_events files.  A time series data that connects holiday, oil_price, and transaction data
# b) train2014 - train2017:  which is inserted in an SQL database

source("./insertFiles-v2.R")
firsttime <- FALSE
if (firsttime == TRUE) {
  create_SQLdatabase()
  insert_train1()
} 

trans_stores_events <- process_events_file()

# 2. Forecast Transaction Data --------------------------------------------
# This file focuses on forecasting the transaction data for each store for the relevant test period
# The objective is to use various controls (holiday,event,pay,oil_prices) to forecast the test period
# The theory is that closely predicting aggregate transactions is far more important than predicting individual trends of products
# The product space per store is too large making estimation of (54 stores)*(~4000 products) too wiedly to use
# standard ARIMA modeling.  The predicted values are inserted in a csv file which can later be retrieved

source("./forecast_store_transactions.R")
fcst_trans <- FALSE
if (fcst_trans == TRUE) {
  predict_transactions()
}

# 3. Forecast Product Data
# This file focuses on forecasting product data for each store for the relevant test period
# It uses the following method:
#   Model 1:
#   a. Obtain average product share in store over 2017 (based on onpromotion price discount)
#   b. Apply this average share to predicted transaction data for test period
#   Model 2:
#   a. Estimate product trend & onpromotion estimates (using matrix multiplication to get OLS estimates)
#   b. forecast forward to time period and apply share estimates to store transaction data (ensuring that values sum to 1)
#   Model 3:
#   a. Exploit cross-sectional time series data

source("./forecast_item_sales.R")



