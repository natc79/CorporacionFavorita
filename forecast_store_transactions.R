# Exploratory modeling on the transaction data
# ----------------------------------------------------------------------
# Purpose of code is to forecast the transaction data using ARIMA modeling
# It is believed that we can exploit the store level transaction data to help estimate
# the unit_sales data at the product level for a given store
# The code currently stores the output in a csv file that can help reduce the need to re-estimate later on


library(GGally)
library(forecast)
library(tseries)
library(lubridate)
library(scales)
library(fpp)
library(xts)


transaction_model <- function(train,test) {
  # Predict transaction level data for test period using auto.arima with various controls
  #args:
  # train is store level transaction dataframe that can be turned into a time series
  # includes various x values for example holiday, payday and other effects
  # test is a dataframe comprised of transactions (may be 0 if not available) and various x values that correspond to it
  
  #return:
  # a series of predicted transaction values for the test period
  
  x <- train[,c("transactions")]
  y <- ts(x, frequency=7)
  z <- fourier(ts(x, frequency=365.25), K=5)
  zf <- fourier(ts(x, frequency=365.25), K=5, h=length(test$date))
  fit <- auto.arima(y, xreg=cbind(z,train$isHoliday,train$isEvent,train$oil_price,train$payday), seasonal=TRUE)
  print(summary(fit))
  fc <- forecast(fit, xreg=cbind(zf,test$isHoliday,test$isEvent,test$oil_price,test$payday), h=length(test$date))
  print(plot(fc))
  # return forecast values (at the mean)
  fc$mean
  
}

predict_transactions <- function() {
  
  # This code loops through the various store data, extracting relevant 
  # test data and then merging the predicted estimates into dataframe that is
  # ultimately written to a csv file
  
  temp_stores <- read_csv("./RawData/stores.csv")
  stores <- temp_stores$store_nbr 
  
  # Set-up the predictions file for the transactions
  pr.dates <- seq(as.Date(max(trans_train$date)+1), as.Date(max(trans_stores_events$date)),by="days")
  num.pr.dates <- length(pr.dates)
  
  # create an events file for entire period of interest (test/train)
  pr_transactions <- data.frame(date = rep(pr.dates, num.all.stores), store_nbr=rep(all.stores, each=num.pr.dates))
  pr_transactions$transactions <- 0
  
  for (i in seq_along(stores)) {
    
    print(paste("Predictions",i, sep=" "))
    
    #get predicted values for the transactions using ARIMA model
    trans_train <- trans_stores_events %>% filter(store_nbr == stores[i] & is.na(trans_stores_events$transactions)==FALSE & date >= "2015-01-01")
    trans_test <- trans_stores_events %>% filter(store_nbr == stores[i] & is.na(trans_stores_events$transactions)==TRUE & date >= max(trans_train$date))
    temp_trans <- transaction_model(trans_train,trans_test)
    
    store_nbr <- rep(stores[i],length(trans_test))
    length(pr.dates)
    temp_data <- as.data.frame(cbind(as.Date(pr.dates),store_nbr,temp_trans))
    colnames(temp_data) <- c("date","store_nbr","temp_transactions")
    temp_data$date <- as.Date(temp_data$date)
    pr_transactions <- pr_transactions %>% left_join(temp_data,by=c("date","store_nbr"))
    pr_transactions$transactions[is.na(pr_transactions$temp_transactions)==FALSE] <- pr_transactions$temp_transactions[is.na(pr_transactions$temp_transactions)==FALSE]
    pr_transactions <- pr_transactions %>% select(-temp_transactions)
  }
  
  # if any of the transactions are less than 0 just set the number to 0 (as we should always have a non-positive number)
  pr_transactions[pr_transactions$transactions < 0,c("transactions")] <-0
  
  # writes the predicted transaction_data and stores it 
  write.csv(pr_transactions,file="./IntermediateData/pr_transactions1.csv")
  
}