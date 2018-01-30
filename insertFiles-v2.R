# DATA INSERTION FILE------------------------------------------------------

# This file contains relevant functions for:
# (i) processing the Corporacion Favorita Data
# (ii) inserting processed data into an SQL database for more rapid querying

library(RSQLite)
library(tidyverse)
library(lubridate)
library(timeDate)

new_train_csv <- function() {
  # This does some initial filtering of the train data that may be easier to read-into SQL
  # going forward we will only use train1.csv
  library(chunked)
  
  min_date <- '2014-04-01'
  # April of 2014 data collection first started for the promotions category.  let's toss out all data before that 
  train_chunk <- read_csv_chunkwise("./RawData/train.csv",chunk_size=1000000,header=TRUE,col.names=cnames, colClasses=cls, sep=",") %>% 
    filter(as.Date(date) >= as.Date(min_date)) %>% select(-id) %>% mutate(unit_sales = round(unit_sales,digits=1), onpromotion = as.integer(onpromotion == "True"))
  
  train_chunk %>% write_csv_chunkwise(file = "./RawData/train1.csv")  
  
}


create_matrix <- function(train,test) {
  #args:
  # train - a data frame from train.csv that is sorted by stores
  # test - a data frame from test.csv that is sorted by stores
  
  test.dates <- unique(test$Date)
  num.test.dates <- length(test.dates)
  all.stores <- unique(test$Store)
  num.stores <- length(all.stores)
  test.depts <- unique(test$Dept)
  #reverse the depts so the grungiest data comes first
  test.depts <- test.depts[length(test.depts):1]
  forecast.frame <- data.frame(date=rep(test.dates, num.items),
                               item_nbr=rep(all.items, each=num.test.dates))
  pred <- test
  pred$unit_sales <- 0
  
  train.dates <- unique(train$date)
  num.train.dates <- length(train.dates)
  train.frame <- data.frame(date=rep(train.dates, num.items),
                            store=rep(all.items, each=num.train.dates))
  
  for(s in test.stores){
    print(paste('stores:', s))
    tr.d <- train.frame
    # This joins in unit_sales but generates some NA's. Resolve NA's 
    # in the model because they are resolved differently in different models.
    tr.d <- join(tr.d, train[train$item_nbr==i, c('store_nbr','date','unit_sales')])
    tr.d <- cast(tr.d, date ~ item_nbr)    
    fc.d <- forecast.frame
    fc.d$unit_sales <- 0
    fc.d <- cast(fc.d, date ~ item_nbr)
    result <- f(tr.d, fc.d, ...)
    # This has all items/Dates for a given store, but may have some that
    # don't go into the submission.
    result <- melt(result)
    pred.d.idx <- pred$store_nbr==s
    #These are the item-Date pairs in the submission for this store
    pred.d <- pred[pred.d.idx, c('item_nbr', 'date')]
    pred.d <- join(pred.d, result)
    pred$unit_sales[pred.d.idx] <- pred.d$value
  }
  pred
    
}


# Processing Functions ----------------------------------------------------

parse_dates <- function(df, datecols) {
  # FUNCTION: Parses dates to create week, month, year values
  for (i in seq_along(datecols)) {
    #print(datecols[i])
    head(df[[datecols[i]]])
    df[[paste(datecols[i],"week",sep="_")]] <- as.integer(strftime(as.POSIXlt(df[[datecols[i]]]),format="%W"))
    df[[paste(datecols[i],"month",sep="_")]] <- as.integer(strftime(as.POSIXlt(df[[datecols[i]]]),format="%m"))  
    df[[paste(datecols[i],"year",sep="_")]] <- as.integer(strftime(as.POSIXlt(df[[datecols[i]]]),format="%Y"))
  }
  df
}

#get critical period
test <- read_csv("./RawData/test.zip", col_names = TRUE)
test_dates <- test %>% summarize(min_date = min(date) - 30,max_date = max(date)+30) %>% parse_dates(c("min_date","max_date"))

convert_colnames <- function(df,prefix) {
  #FUNCTION:  Adds a prefix for the datasets in holidays_events
  
  new_colnames <- vector("character", ncol(df))
  old_colnames <- colnames(df)
  for (i in seq_along(old_colnames)) {
    if (old_colnames[i] %in% c("date","city","state","natl")) {
      new_colnames[i] <- old_colnames[i]
    }
    else {
      new_colnames[i] <- paste(prefix,old_colnames[i],sep="_")
    }
  }
  new_colnames
}

process_chunk <- function(chunk,old_extra) {
  #FUNCTION:  processes each chunk of data
  
  #bind the new chunk with the old_extra chunk
  temp_chunk <- rbind(chunk,old_extra)
  temp_chunk <- temp_chunk %>% parse_dates(c("date"))
  #get cutoff date of data chunk
  temp <- temp_chunk %>% summarize(cutoff_date = as.Date(max(date))-7) %>% parse_dates(c("cutoff_date"))

  #filter the data to make sure our data represents a complete week
  chunk_grouping <- temp_chunk %>% filter((date_week <= temp$cutoff_date_week & date_year == temp$cutoff_date_year) | date_year < temp$cutoff_date_year)
  #print("FILTER 1")
  chunk_extra <- temp_chunk %>% filter((date_week > temp$cutoff_date_week & date_year == temp$cutoff_date_year) | date_year > temp$cutoff_date_year) %>%
    select(id,date,store_nbr,item_nbr,unit_sales,onpromotion)
  #print("FILTER 2")
  temp_dates <- chunk_grouping %>% summarize(max_date = max(date), min_date =min(date))
  #filter transactions
  trans_grouping <- trans_stores_events %>% filter(temp_dates$min_date <= date & date <= temp_dates$max_date) %>%
    select(date,date_year,date_month,store_nbr,transactions) %>% arrange(date_year,date_month,store_nbr)
  #print("FILTER 3")
  monthly_items <- chunk_grouping %>% group_by(date_year,date_month, store_nbr, item_nbr) %>% summarize(cnt=n())
  temp_grouping <- trans_grouping %>% left_join(monthly_items,by=c("date_year","date_month","store_nbr")) %>% select("date","date_year","date_month","store_nbr","item_nbr","transactions")
  temp_grouping <- unique(temp_grouping)
  #we can remerge temp now with our training set our objective is to get distinct product numbers
  chunk_grouping <- temp_grouping %>% left_join(chunk_grouping, by=c("date","date_year","date_month","store_nbr","item_nbr"))
  arrange(chunk_grouping,store_nbr,item_nbr,date)
  #fill-in all unit sales that are missing with 0
  chunk_grouping["unit_sales"][is.na(chunk_grouping["unit_sales"]) == TRUE] <- 0

  #return chunk-grouping (weekly), plus the chunk_extra
  chunk_weekly <- chunk_grouping %>% group_by(date_year,date_week,store_nbr,item_nbr) %>% summarize(mean_sales = mean(unit_sales), mean_onpromotion = mean(onpromotion), mean_store_transactions = mean(transactions))
  #chunk critical daily (1 month period around test dates)
  #print(sapply(chunk_grouping,class))
  chunk_daily <- chunk_grouping %>% filter(test_dates$min_date_week <= date_week & date_week <= test_dates$max_date_week) %>%
    select(id, date,store_nbr,item_nbr,unit_sales,onpromotion)
  #print("FILTER 4")
  return(list(chunk_weekly,chunk_daily,chunk_extra))
}

process_events_file <- function() {
  #FUNCTION: process events file by combining it with relevant data
  
  # stores files which will be used to set levels in the events file
  stores <- read_csv("./RawData/stores.csv", col_names = TRUE)
  stores <- stores %>% mutate(city1 = city, state1 = state, city = as_factor(city1), state = as_factor(state1), type = as_factor(type))
  stores <- stores %>% select(-city1,-state1)
  head(stores)
  levels_city = levels(stores$city)
  levels_state = levels(stores$state)
  
  test <- read_csv("./RawData/test.zip", col_names = TRUE)
  all.stores <- stores$store_nbr
  num.all.stores <- length(all.stores)
  transactions <- read_csv("./RawData/transactions.csv", col_names = TRUE)
  min_date <- min(transactions$date)
  max_date <- max(test$date)
  
  all.dates <- seq(as.Date(as.character(min_date)),as.Date(as.character(max_date)),by="days")
  num.all.dates <- length(all.dates)
  
  # create an events file for entire period of interest (test/train)
  df_events <- data.frame(date = rep(all.dates, num.all.stores), store_nbr=rep(all.stores, each=num.all.dates))
  
  # oil file (fill in the oil prices with the values from last date if missing)
  oil <- read_csv("./RawData/oil.csv", col_names = TRUE)
  oil <- oil %>% mutate(oil_price = dcoilwtico) %>% select(-dcoilwtico)
  date <- seq(min(oil$date),max(oil$date),by="days")
  oil <- as.data.frame(date) %>% left_join(oil,by="date")
  library(zoo)
  oil<- zoo(oil)
  oil <- na.locf(oil,fromLast=TRUE)
  oil <- as.data.frame(oil)
  oil$date <- as.Date(oil$date)
  
  events <- read_csv("./RawData/holidays_events.csv", col_names = TRUE)
  
  #process events_city
  events_city <- events %>% filter(locale == 'Local') %>% select(-locale) 
  #for now we will just assume at city level "transfer", "additional", "holiday" are the same
  events_city <- events_city %>% mutate(isHoliday = as.integer((type == "Holiday")), isAdditional = as.integer((type == "Additional")), isTransfer = as.integer((type == "Transfer")), city = factor(locale_name, levels = levels_city), transferred = as.integer((transferred == "True")))%>%
    select(-description,-type,-locale_name)
  #sometimes there is multiple holidays/events entered for a given day so collapse over the days
  ?summarize
  events_city <- events_city %>% group_by(date,city) %>% summarize_all(max)
  new_colnames <- convert_colnames(events_city,"city")
  colnames(events_city) <- new_colnames
  
  #process events_state
  events_state <- events %>% filter(locale == 'Regional') %>% mutate(isHoliday = (type == "Holiday"), state = factor(locale_name, levels = levels_state)) %>% select(-locale, -transferred,-description,-type,-locale_name)
  head(events_state)
  events_state <- events_state %>% group_by(date,state) %>% summarize_all(max)
  new_colnames <- convert_colnames(events_state,"state")
  new_colnames
  colnames(events_state) <- new_colnames
  
  #process events_national
  events_national <- events %>% filter(locale == 'National') 
  unique(events_national$type)
  events_national <- events_national %>% 
    mutate(isHoliday = as.integer((type == "Holiday")), isTransfer = as.integer((type == "Transfer")), isAdditional = as.integer((type == "Additional")), isBridge = as.integer((type == "Bridge")), isWorkDay = as.integer((type == "Work Day")), isEvent = as.integer((type == "Work Day")), transferred = as.integer((transferred == "True"))) %>%
    select(-description,-type,-locale_name,-locale) 
  head(events_national)
  events_national <- events_national %>% group_by(date) %>% summarize_all(max)
  new_colnames <- convert_colnames(events_national,"natl")
  new_colnames
  colnames(events_national) <- new_colnames
  events_national <- events_national
  
  # Get the transaction file and combine transactions with holidays, stores, events file
  trans <- read_csv("./RawData/transactions.csv", col_names = TRUE)
  head(trans)
  # merge transaction file with our main dataframe values
  trans <- df_events %>% left_join(trans,by=c("date","store_nbr")) 
  
  trans_stores <- left_join(trans, stores, by="store_nbr")
  trans_stores_events <- trans_stores %>% left_join(events_city, by=c("date","city")) %>% left_join(events_state, by=c("date","state")) %>% left_join(events_national, by=c("date")) %>% left_join(oil,by=c("date"))
  trans_stores_events[,8:length(trans_stores_events)][is.na(trans_stores_events[,8:length(trans_stores_events)])] <- 0
  
  trans_stores_events$date_eom <- as.Date(timeLastDayInMonth(trans_stores_events$date))
  trans_stores_events$payday <-  ifelse((day(trans_stores_events$date) == 15) | (day(trans_stores_events$date) == day(trans_stores_events$date_eom)), 1, 0)
  
  trans_stores_events <- trans_stores_events %>% select(-date_eom)
  #can collapse across rows
  trans_stores_events <- trans_stores_events %>% mutate(isHoliday = rowSums(trans_stores_events[c("natl_isHoliday","state_isHoliday","city_isHoliday")]))
  trans_stores_events$isHoliday[trans_stores_events$isHoliday > 1] <- 1
  trans_stores_events <- trans_stores_events %>% mutate(isTransfer = rowSums(trans_stores_events[c("natl_isTransfer","city_isTransfer")]))
  trans_stores_events$isHoliday[trans_stores_events$isTransfer > 1] <- 1
  trans_stores_events <- trans_stores_events %>% mutate(isAdditional = rowSums(trans_stores_events[c("natl_isAdditional","city_isAdditional")]))
  trans_stores_events$isHoliday[trans_stores_events$isAdditional > 1] <- 1
  trans_stores_events <- trans_stores_events %>% parse_dates(c("date"))
  
  sql_trans_stores_events <- trans_stores_events
  sql_trans_stores_events$date <- as.character(sql_trans_stores_events$date)
  dbcon <- dbConnect(RSQLite::SQLite(), "./corporacionfavorita.db")
  dbWriteTable(dbcon,"trans_stores_events",trans_stores_events,overwrite=TRUE)
  dbDisconnect(dbcon)
  
  trans_stores_events
}

test_chunk <- function() {
  
  #set the colnames
  cls <- c('integer','Date','integer', 'integer','double', 'character')
  colnames <- c("id","date","store_nbr","item_nbr","unit_sales","onpromotion")
  
  #check that read.table is working like a tibble
  chunk <- read.table("./RawData/train.csv", header = FALSE, col.names=colnames,sep = ",", skip = 15000000, nrows = 100000, colClasses=cls, na.strings = "NA")
  head(chunk)
  unique(chunk$onpromotion)
  
  chunk <- read.table("./RawData/train.csv", header = FALSE, col.names=colnames,sep = ",", skip = 30000000, nrows = 100000, colClasses=cls, na.strings = "NA")
  head(chunk)
  unique(chunk$onpromotion)
  
  ?data.frame
  old_chunk <- read.table("./RawData/train.csv", header = FALSE, col.names=colnames,sep = ",", skip=1, nrows = 1, colClasses=cls)
  old_chunk <- old_chunk %>% filter(date < "2013-01-01")
  head(old_chunk)
  new_chunk <- chunk %>% select(-id) %>% mutate(onpromotion = as.integer(onpromotion == "True")) 
  datalist <- process_chunk(new_chunk,old_chunk)
}

train_temp <- read_csv("./RawData/train1.csv", col_names = TRUE, n_max=1000000)
train_temp <- read_csv("./RawData/train1.csv",n_rows=1000000)
chunk_extra <- read.table("./RawData/train1.csv", header = FALSE, col.names=cnames,sep = ",", skip=1, nrows = 1, colClasses=cls)

insert_train2 <- function() {
  
  dbcon <- dbConnect(RSQLite::SQLite(), "./corporacionfavorita.db")

  filename <- "./RawData/train1.csv"
  max_rows <- 1000000
  con <- file(description=filename, open="r")
  
  cls <- c('Date','integer', 'integer','double', 'integer')
  cnames <- c("date","store_nbr","item_nbr","unit_sales","onpromotion")
  
  chunk <- read.table(con, header = FALSE, col.names=cnames, sep = ",", skip=1, nrows = max_rows, colClasses=cls)
  
  i <- 0
  repeat {
    i <- i + 1
    print(paste("Iteration",toString(i),sep=" "))
    if (nrow(chunk) == 0)
      break
    # process chunk of data here (create some basic variables...etc)
    chunk$date <- as.character(chunk$date)
    # Insertion of files into SQL database
    
    yearlist <- unique(year(chunk$date))
    for (j in seq_along(yearlist)) {
      chunk_subset <- chunk %>% filter(year(chunk$date) == yearlist[j])
      dbWriteTable(conn=dbcon,name=paste("train",yearlist[j],sep=""),chunk_subset, append=T, row.names=FALSE)
    }
    
    if (nrow(chunk) != max_rows)
      break
    
    # Alternatively read in the data
    chunk <- tryCatch({read.table(con, header = FALSE, col.names=cnames, sep = ",", skip=0, nrows = max_rows, colClasses=cls) },
                      error=function(err) {
                        if (identifical(conditionMessage(err), "no lines available in input"))
                          data.frame()
                        else stop(err)
                      })
  }
  dbDisconnect(dbcon)  
}

#insert_train2()


insert_train <- function() {
  #FUNCTION:  inserts the training dataset into an SQL database
  
  dbcon <- dbConnect(RSQLite::SQLite(), "./corporacionfavorita.db")
  
  filename <- "./RawData/train1.csv"
  max_rows <- 1000000
  con <- file(description=filename, open="r")
  
  cls <- c('Date','integer', 'integer','double', 'integer')
  cnames <- c("date","store_nbr","item_nbr","unit_sales","onpromotion")
  
  chunk_extra <- read.table("./RawData/train1.csv", header = FALSE, col.names=cnames,sep = ",", skip=1, nrows = 1, colClasses=cls)
  chunk_extra <- chunk_extra %>% filter(date < "2013-01-01")

  chunk <- read.table(con, header = FALSE, col.names=cnames, sep = ",", skip=1, nrows = max_rows, colClasses=cls)
  i <- 0
  repeat {
    i <- i + 1
    print(paste("Iteration",toString(i),sep=" "))
    if (nrow(chunk) == 0)
      break
    # process chunk of data here (create some basic variables...etc)
    datalist <- process_chunk(chunk,chunk_extra)
    chunk_weekly <- datalist[[1]]
    chunk_daily <- datalist[[2]]
    chunk_extra <- datalist[[3]]
    chunk_daily$date <- as.character(chunk_daily$date)
    # Insertion of files into SQL database
    print(head(chunk_weekly))
    dbWriteTable(conn=dbcon,name="train_weekly",chunk_weekly, append=T, row.names=FALSE)
    print(head(chunk_daily))
    dbWriteTable(conn=dbcon,name="train_daily", chunk_daily, append=T, row.names=FALSE)
    
    #chunk_extra <- chunk_extra[cnames]
    #print(head(chunk_extra))
    
    if (nrow(chunk) != max_rows)
      break
    
    # Alternatively read in the data
    chunk <- tryCatch({read.table(con, header = FALSE, col.names=cnames, sep = ",", skip=0, nrows = max_rows, colClasses=cls) },
                      error=function(err) {
                        if (identifical(conditionMessage(err), "no lines available in input"))
                          data.frame()
                        else stop(err)
                      })
  }
  dbDisconnect(dbcon)
}

create_SQLdatabase1 <- function() {
  #Function resets the tables for our database

  dbcon <- dbConnect(RSQLite::SQLite(), "./corporacionfavorita.db")

  yearlist = c(2014,2015,2016,2017)
  for (i in seq_along(yearlist)) {
    part1 <- "CREATE TABLE"
    table1 <- paste("train",yearlist[i]," (",sep="")
    part2 <- "date TEXT, store_nbr INTEGER, item_nbr INTEGER,unit_sales DOUBLE,onpromotion INTEGER,
    PRIMARY KEY(date, store_nbr, item_nbr));"
    query <- paste(part1,table1,part2,sep=" ")
    print(query)
    dbSendQuery(conn=dbcon,query)
  }
  
  dbListTables(dbcon)
  #dbCommit(dbcon)
  dbDisconnect(dbcon)
}

#create_SQLdatabase1()

# BASIC QUERYING FROM SQL DATABASE
extra <- function() {

  dbListTables(dbcon)
  dbListFields(dbcon,"train_weekly")
  
  dbcon <- dbConnect(RSQLite::SQLite(), "./corporacionfavorita.db")
  query = "SELECT year, MAX(date_week) from train_weekly GROUP BY date_year;"
  dbGetQuery(conn=dbcon,query) 
  
  dbListFields(dbcon,"train_daily")
  query = "SELECT MAX(date) from train_daily WHERE DATE(date) >= DATE('2017-07-01');"
  dbGetQuery(conn=dbcon,query) 
  
  dbDisconnect(dbcon)

}

