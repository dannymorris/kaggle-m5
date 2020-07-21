# This script generates product-level training data.
# Each individual training data set (by product) is uploaded to S3 as a CSV file

## Run this on EC2 using RStudio AMI

Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           "AWS_SECRET_ACCESS_KEY" = "",
           "AWS_DEFAULT_REGION" = "us-east-1")

setwd("~/kaggle-m5")

###############
## Libraries ##
###############

install.packages("recipes")
install.packages("zoo")
install.packages("data.table")
install.packages("aws.s3", repos = "https://cloud.R-project.org")

library(tidyverse)
library(recipes) 
library(lubridate)
library(zoo)
library(parallel)
library(data.table)
library(aws.s3)

###############
## Functions ##
###############

label_enc <- function(x) {
  as.numeric(x)
}

replace_na <- function(x) {
  ifelse(is.na(x), 0, x)
}

nearest_val <- function(x) {
  data.table::nafill(x, "locf")
}

##############
## Raw data ##
##############

# calendar reference
calendar <- s3read_using(
  object = "m5_store_items/raw_data/calendar.csv",
  bucket = "abn-distro",
  FUN = fread
) %>%
  as_tibble() %>%
  mutate(date = as.Date(date)) %>%
  filter(date >= "2012-01-01") %>%
  filter((month %in% c(3,4,5,6)) | year == 2016) 

train_d <- calendar$d[1:631]
future_d <- paste("d", 1942:1969, sep = "_")

# prices
prices <- s3read_using(
  object = "m5_store_items/raw_data/sell_prices.csv",
  bucket = "abn-distro",
  FUN = fread
) %>%
  semi_join(calendar, by = "wm_yr_wk") %>%
  mutate(store_item = paste(item_id, store_id, sep = "_")) %>%
  as_tibble()

# sales
eval_df <- s3read_using(
  object = "m5_store_items/raw_data/sales_train_evaluation.csv",
  bucket = "abn-distro",
  FUN = fread
) %>%
  select(id:state_id, all_of(train_d)) %>%
  as_tibble()

range(sample_df$date)

#################
## Item encode ##
#################

item_enc <- eval_df %>%
  select(id, item_id, dept_id, cat_id, store_id, state_id) %>%
  mutate_at(vars(-id), as.factor) %>%
  mutate_at(vars(-id), label_enc)

#####################
## Calendar encode ##
#####################

# one-hot enconde date components
# weekday, weekmonth, month, year
date_enc <- calendar %>%
  mutate(weekmonth = stringi::stri_datetime_fields(date)$WeekOfMonth) %>%
  mutate(week = lubridate::week(date)) %>%
  mutate(quarter = lubridate::quarter(date)) %>%
  mutate(day = lubridate::day(date)) %>%
  select(weekday, weekmonth, month, year, quarter, week, date) %>%
  mutate_at(vars(-date), as.factor) %>%
  mutate_at(vars(-date), label_enc)

#####################
## Holidays encode ##
#####################

holidays_enc <- calendar %>%
  select(date, contains("event")) %>%
  replace(is.na(.), "unknown") %>%
  mutate_at(vars(-date), as.factor) %>%
  mutate_at(vars(-date), label_enc) 

holiday_lead <- map(seq(1, 2, 1), function(val) {
  lead_val <- function(x) lead(x, val)
  holidays_enc %>%
    select(-date) %>%
    mutate_all(list(lead_val)) %>%
    setNames(., nm = paste(names(.), "Lead", val, sep = "_")) 
}) %>%
  bind_cols()

holidays_df <- bind_cols(holidays_enc, holiday_lead)


#################
## Sports data ##
#################

nba_finals <-  s3read_using(
  object = "m5_store_items/raw_data/nba_finals.csv",
  bucket = "abn-distro",
  FUN = read_csv
)

# US Open (Sundays)
us_open <-  s3read_using(
  object = "m5_store_items/raw_data/us_open.csv",
  bucket = "abn-distro",
  FUN = read_csv
) %>%
  mutate(date = lubridate::mdy(date)) %>%
  mutate(USO_Sunday = 1) %>%
  select(-event)

# MLB schedule for teams in WI, CA, and TX
mlb <-  s3read_using(
  object = "m5_store_items/raw_data/mlb_games.csv",
  bucket = "abn-distro",
  FUN = read_csv
)

# Horse racing (triple crown)
horse_race <-  s3read_using(
  object = "m5_store_items/raw_data/horse_racing.csv",
  bucket = "abn-distro",
  FUN = read_csv
) %>%
  mutate(date = lubridate::mdy(date)) %>%
  mutate(Horse_Race = 1) %>%
  select(-event) %>%
  distinct()

# Combine football, nba, us open, mlb, and horse racing
sports_df <- nba_finals %>%
  left_join(us_open, by = "date") %>%
  left_join(mlb, by = "date") %>%
  left_join(horse_race, by = "date") %>%
  replace(is.na(.), 0)

################
## Items list ##
################

calendar_enc <- calendar %>%
  select(date, d, wm_yr_wk, contains("snap")) %>%
  inner_join(date_enc, by = "date") %>%
  inner_join(holidays_df, by = "date") %>%
  left_join(sports_df, by = "date")

sample_df <- eval_df %>%  
  select(item_id, store_id, contains("d_")) %>%
  gather(Date, Value, -item_id, -store_id) %>%
  # left_join(calendar_enc, ., by = c("d" = "Date")) %>%
  # mutate_at(vars(NBA_Finals:Horse_Race), replace_na) %>%
  left_join(prices, ., by = c("store_id", "item_id", "wm_yr_wk")) 
  #select(store_item, date, d, Value, contains("snap"), weekday:sell_price)

demand_long <- eval_df %>% 
  select(item_id, store_id, contains('d_')) %>%
  gather(d, Value, -item_id, -store_id)

sample_df <- prices %>%
  inner_join(calendar %>% select(wm_yr_wk, date, d), by = "wm_yr_wk") %>%
  left_join(demand_long, by = c("store_id", "item_id", "d"))

aws.s3::s3write_using(
  x = sample_df,
  FUN = fwrite,
  bucket = "abn-distro",
  object = "sample_df.csv"
)
# separate all items using list
# modeling will use split-apply-combine method
items_list <- sample_df %>% split(., .$store_item)

rm(sample_df)
rm(prices)
gc()

###########
## Model ##
###########

n_cores <- parallel::detectCores()-4
n_cores
cl <- makeCluster(n_cores)
clusterExport(cl, c("ohe_calendar",
                    "future_calendar",
                    "ohe_sports",
                    "future_sports_df",
                    "future_prices"))

start_time <- Sys.time()

#df <- items_list[[31]]

item_tbls <- parallel::parLapply(cl, items_list, function(df) {
  
  library(tidyverse)
  library(lubridate)
  library(zoo)
  
  future_df <- df %>%
    filter(date >= "2016-05-23")
  
  # future leads
  future_lead_vars <- future_calendar %>%
    inner_join(future_sports_df, by = "date") %>%
    select(date, 
           contains('event'),
           contains("MLB"),
           NFL_Sun,
           NCAA_Sat,
           NBA_Finals,
           USO_Sunday,
           contains("Horse"))
  
  future_lead_tbls <- map(seq(1, 2, 1), function(val) {
    lead_val <- function(x) lead(x, val)
    future_lead_vars %>%
      select(-date) %>%
      mutate_all(list(lead_val)) %>%
      setNames(., nm = paste(names(.), "Lead", val, sep = "_"))
  }) %>%
    bind_cols(future_lead_vars, .)
  
  
  #################################
  ## Calendar lagging indicators ##
  #################################
  
  # calendar events that influence future sales
  # lags up to 1 day
  # variables: Black Friday, post-Christmas
  calendar_lag <- point_indicators %>%
    select(date,
           event_Thanksgiving,
           event_Christmas) %>%
    distinct() %>%
    mutate(event_Thanksgiving_Lag_1 = lag(event_Thanksgiving, 1),
           event_Christmas_Lag_1 = lag(event_Christmas, 1)) %>%
    select(date,
           event_Thanksgiving_Lag_1,
           event_Christmas_Lag_1)
  
  ###############################
  ## Calendar fixed indicators ##
  ###############################
  
  # point indicators with no lead/lag component
  calendar_fixed <- point_indicators %>%
    select(date,
           contains("weekday"),
           contains("month"),
           contains("year"),
           contains("weekmonth"),
           -contains("event")) %>%
    distinct()
  
  # future fixed
  future_fixed_tbls <- future_calendar %>%
    select(date,
           contains("snap"),
           contains("weekday"),
           contains("month"),
           contains("year"),
           contains("weekmonth"),
           -contains("event"))
  
  ###############
  # subset SNAP #
  ###############
  
  # get state SNAP indicators
  item_snap <- point_indicators %>%
    select(date, contains("snap")) 
  
  ##############
  # subset MLB #
  ##############
  
  # get MLB games in item state
  item_mlb_state <- calendar_lead %>%
    select(date, contains("MLB"))
  
  ###############
  # Item prices #
  ###############
  
  get_rollmean <- function(x) {
    zoo::rollmean(x, ceiling(length(x)*.12), fill = NA, align = 'right')
  }
  
  # point price with lag-1
  item_prices <- df %>%
    mutate(roll_mean = get_rollmean(sell_price)) %>%
    mutate(roll_mean = ifelse(is.na(roll_mean), sell_price, roll_mean)) %>%
    mutate(promo = ifelse((sell_price - roll_mean)/roll_mean <= -0.02, 1, 0)) %>%
    mutate(hike = ifelse((sell_price - roll_mean)/roll_mean >= 0.02, 1, 0)) %>%
    mutate(sell_price_Lag_1 = lag(sell_price, 1)) %>%
    mutate(sell_price_diff = sell_price - lag(sell_price, 1)) %>%
    mutate(sell_price_change = ((lag(sell_price) - sell_price)/sell_price)) %>%
    select(date,
           promo,
           hike)
  
  #########################
  # Rolling sales metrics #
  #########################
  
  # moving medians (7, 14, and 30 day)
  # moving standard devs (7, 14, and 30 day)
  # moving stats backshifted by 30 days to enable 30-day forecasts
  item_rolling <- df %>%
    select(date, Value) %>%
    mutate(Roll_Mean_28 = zoo::rollmean(Value, 28, align = 'right', fill = NA)) %>%
    mutate(Roll_Mean_7 = zoo::rollmean(Value, 7, align = 'right', fill = NA)) %>%
    mutate(Roll_Mean_14 = zoo::rollmean(Value, 14, align = 'right', fill = NA)) %>%
    mutate(Roll_Mean_28_Shift = lag(Roll_Mean_28, 28)) %>%
    mutate(Roll_Mean_7_Shift = lag(Roll_Mean_7, 28)) %>%
    mutate(Roll_Mean_14_Shift = lag(Roll_Mean_14, 28)) %>%
    mutate(Roll_Sd_28 = zoo::rollapply(Value, 28, sd, align = 'right', fill = NA)) %>%
    mutate(Roll_Sd_7 = zoo::rollapply(Value, 7, sd, align = 'right', fill = NA)) %>%
    mutate(Roll_Sd_14 = zoo::rollapply(Value, 14, sd, align = 'right', fill = NA)) %>%
    mutate(Roll_Sd_28_Shift = lag(Roll_Sd_28, 28)) %>%
    mutate(Roll_Sd_7_Shift = lag(Roll_Sd_7, 28)) %>%
    mutate(Roll_Sd_14_Shift = lag(Roll_Sd_14, 28)) %>%
    select(date, 
           contains("Shift"))
  
  ##################
  # Modeling table #
  ##################
  
  # combine prices, SNAP, rolling stats, holidays, dates, leads, lags
  # extra indicator for zero demand
  model_tbl <- df %>%
    #select(-contains("snap")) %>%
    inner_join(item_prices, by = "date") %>%
    inner_join(item_snap, by = "date") %>%
    inner_join(item_rolling, by = "date") %>%
    inner_join(calendar_lead, by = "date") %>%
    select(-contains("MLB")) %>%
    inner_join(item_mlb_state, by = "date") %>%
    inner_join(calendar_lag, by = "date") %>%
    inner_join(calendar_fixed, by = "date") %>%
    drop_na() %>%
    select(date,
           id,
           store_item,
           dept_id,
           cat_id,
           store_id,
           state_id,
           contains("snap_"),
           contains("year_"),
           contains("month_"),
           contains("weekday_"),
           contains("weekmonth_"),
           contains("NFL_"),
           contains("NBA_"),
           contains("NCAA_"),
           contains("USO_"),
           contains("MLB_"),
           contains("Horse_"),
           contains("event_"),
           contains("Roll_"),
           sell_price,
           promo,
           hike,
           Value) 
  
  #################
  # Training data #
  #################
  
  train <- model_tbl %>%
    filter(date >= "2013-01-01") 
  
  ############
  # Forecast #
  ############
  
  # future prices
  future_item_prices <- future_prices %>%
    semi_join(df, by = c("store_item")) %>%
    select(date, sell_price) %>%
    bind_rows(df %>% select(date, sell_price)) %>%
    distinct() %>%
    arrange(date) %>%
    mutate(roll_mean = get_rollmean(sell_price)) %>%
    mutate(roll_mean = ifelse(is.na(roll_mean), sell_price, roll_mean)) %>%
    mutate(promo = ifelse((sell_price - roll_mean)/roll_mean <= -0.02, 1, 0)) %>%
    mutate(hike = ifelse((sell_price - roll_mean)/roll_mean >= 0.02, 1, 0)) %>%
    semi_join(future_prices, by = "date")
  
  # future lags
  future_lag_tbls <- inner_join(future_calendar, future_item_prices, by = "date") %>%
    mutate(sell_price_Lag_1 = lag(sell_price, 1),
           event_Thanksgiving_Lag_1 = lag(event_Thanksgiving, 1),
           event_Christmas_Lag_1 = lag(event_Christmas, 1)) %>%
    select(date, 
           sell_price,
           sell_price_Lag_1,
           event_Thanksgiving_Lag_1,
           event_Christmas_Lag_1)
  
  # future rolling
  future_rolling <- df %>%
    arrange(date) %>%
    tail(60) %>%
    mutate(Roll_Mean_28_Shift = zoo::rollmean(Value, 28, align = 'right', fill = NA)) %>%
    mutate(Roll_Mean_7_Shift = zoo::rollmean(Value, 7, align = 'right', fill = NA)) %>%
    mutate(Roll_Mean_14_Shift = zoo::rollmean(Value, 14, align = 'right', fill = NA)) %>%
    mutate(Roll_Sd_28_Shift = zoo::rollapply(Value, 28, sd, align = 'right', fill = NA)) %>%
    mutate(Roll_Sd_7_Shift = zoo::rollapply(Value, 7, sd, align = 'right', fill = NA)) %>%
    mutate(Roll_Sd_14_Shift = zoo::rollapply(Value, 14, sd, align = 'right', fill = NA)) %>%
    select(date, Roll_Mean_28_Shift:Roll_Sd_14_Shift) %>%
    drop_na() %>%
    mutate(date = date + days(28))
  
  # future modeling table
  future_model_tbl <- future_calendar %>%
    select(date) %>%
    mutate(id = unique(df$id),
           store_item = unique(df$store_item),
           item_id = unique(df$item_id),
           dept_id = unique(df$dept_id),
           cat_id = unique(df$cat_id),
           store_id = unique(df$store_id),
           state_id = unique(df$state_id)) %>%
    inner_join(future_item_prices %>% select(date, hike, promo), by = "date") %>%
    inner_join(future_rolling, by = "date") %>%
    inner_join(future_lead_tbls, by = "date") %>%
    inner_join(future_lag_tbls, by = "date") %>%
    inner_join(future_fixed_tbls, by = "date") %>%
    select(colnames(model_tbl %>% select(-Value))) %>%
    replace(is.na(.), 0) 
  
  train <- model_tbl %>% 
    select(-date, -id, -store_item) %>%
    select(Value, everything())
  
  eval <- future_model_tbl %>%
    select(-date, -id, -store_item) %>%
    select(everything())
  
  eval_meta <- future_model_tbl %>%
    select(date, id, store_id)
  
  file_id <- unique(df$id)
  train_file_name <- paste("walmart_items/train/", file_id, ".csv", sep = "")
  eval_file_name <- paste("walmart_items/eval/", file_id, ".csv", sep = "")
  eval_meta_file_name <- paste("walmart_items/eval_meta/", file_id, ".csv", sep = "")
  
  out <- list(
    train = train,
    eval = eval,
    eval_meta = eval_meta,
    store_id = unique(df$store_id)
  )
  
  rm(calendar_fixed)
  rm(calendar_lag)
  rm(point_indicators)
  rm(model_tbl)
  rm(train)
  
  return(out)
  
})

stopCluster(cl)
rm(cl)
gc()

n_cores <- parallel::detectCores()-4
n_cores
cl <- makeCluster(n_cores, type = "MPI")

# load training to s3
parallel::parLapply(cl, item_tbls, function(item) {
  train <- item[['train']]
  file_id <- unique(item[['eval_meta']]$id)
  file_name <- paste("m5_store_items/train/", file_id, ".csv", sep = "")
  #write_csv(train, file_name)
  aws.s3::s3write_using(
    x = train,
    FUN = readr::write_csv,
    object = file_name,
    bucket = "abn-distro"
  )
})

# load eval to s3
parallel::parLapply(cl, item_tbls, function(item) {
  eval <- item[['eval']]
  file_id <- unique(item[['eval_meta']]$id)
  file_name <- paste("m5_store_items/eval/", file_id, ".csv", sep = "")
  aws.s3::s3write_using(
    x = eval,
    FUN = data.table::fwrite,
    object = file_name,
    bucket = "abn-distro"
  )
})

# load eval meta to s3
parallel::parLapply(cl, item_tbls, function(item) {
  eval_meta <- item[['eval_meta']]
  file_id <- unique(item[['eval_meta']]$id)
  file_name <- paste("m5_store_items/eval_meta/", file_id, ".csv", sep = "")
  aws.s3::s3write_using(
    x = eval_meta,
    FUN = data.table::fwrite,
    object = file_name,
    bucket = "abn-distro"
  )
})

stopCluster(cl)
gc()

