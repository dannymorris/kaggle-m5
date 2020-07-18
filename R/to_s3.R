

## Run this on EC2 using RStudio AMI

install.packages("recipes")
install.packages("zoo")
install.packages("data.table")
install.packages("aws.s3", repos = "https://cloud.R-project.org")

Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           "AWS_SECRET_ACCESS_KEY" = "",
           "AWS_DEFAULT_REGION" = "us-east-1")


library(tidyverse)
library(recipes) 
library(lubridate)
library(zoo)
library(parallel)
library(data.table)


setwd("~/kaggle-m5")

# calendar reference
calendar <- read_csv("data/calendar.csv")

# prices
prices <- read_csv("data/sell_prices.csv") %>%
  mutate(store_item = paste(item_id, store_id, sep = "_")) %>%
  select(-item_id,
         -store_id)

# training set
eval_df <- read_csv("data/sales_train_evaluation.csv") %>%
  mutate(store_item = paste(item_id, store_id, sep = "_"))

sample_df <- eval_df %>%
  gather(Date, Value, -id:-state_id, -store_item) %>%
  inner_join(calendar, by = c("Date" = "d")) %>%
  left_join(prices, by = c("store_item", "wm_yr_wk")) %>%
  select(id,
         item_id,
         dept_id,
         cat_id,
         store_id,
         state_id,
         Date,
         wm_yr_wk,
         Value,
         date,
         store_item,
         weekday,
         month)

range(sample_df$date)

###########
## Items ##
###########

label_enc <- function(x) {
  as.numeric(x)-1
}

item_ohe <- eval_df %>%
  select(store_item, item_id, dept_id, cat_id, store_id, state_id) %>%
  mutate_at(vars(-store_item),as.factor) %>%
  mutate_at(vars(-store_item),label_enc)
# mutate(item_id = as.numeric(item_id)-1) %>%
# recipes::recipe(~., data = .) %>%
# step_dummy(all_predictors(), -store_item, -item_id, one_hot = T) %>%
# prep() %>%
# juice()

##############
## Calendar ##
##############

# one-hot enconde date components
# weekday, weekmonth, month, year
ohe_date <- calendar %>%
  mutate(weekmonth = stringi::stri_datetime_fields(date)$WeekOfMonth) %>%
  select(weekday, weekmonth, month, year) %>%
  mutate_all(as.factor) %>%
  mutate_all(label_enc)

# one-hot encode holidays
ohe_holidays <- calendar %>%
  select(date, contains("event")) %>%
  gather(key, event, -date) %>%
  drop_na() %>%
  select(-key) %>%
  mutate(Ind = 1) %>%
  mutate(event = str_replace_all(event, " ", "")) %>%
  mutate(event = str_replace_all(event, "[^[:alnum:]]", "")) %>%
  mutate(event = paste("event", event, sep = "_")) %>%
  distinct() %>%
  spread(event, Ind, fill = 0) 

# combine OHE dates and holidays
ohe_calendar <- calendar %>%
  select(date, d, wm_yr_wk, contains("snap")) %>%
  bind_cols(ohe_date) %>%
  left_join(ohe_holidays, by = "date") %>%
  replace(is.na(.), 0)

# future calendar dates
future_calendar <- ohe_calendar %>%
  filter(date >= "2016-05-23")

range(future_calendar$date)


##########
# Prices #
##########

future_prices <- prices %>%
  inner_join(future_calendar %>%
               select(date, wm_yr_wk), 
             by = "wm_yr_wk")

range(future_prices$date)

#################
## Sports data ##
#################

# NFL Sunday and College football Saturday
football <- calendar %>%
  select(date, weekday, month) %>%
  distinct() %>%
  mutate(NFL_Sun = ifelse(weekday == "Sunday" & (month > 8 | month < 2), 1, 0)) %>%
  mutate(NCAA_Sat = ifelse(weekday == "Saturday" & (month > 8 & month <= 12), 1, 0)) %>%
  select(date, NFL_Sun, NCAA_Sat) %>%
  distinct()

# NBA Finals schedule
# nba_finals <- nbastatR::seasons_schedule(2011:2016, season_types = 'Playoffs') %>%
#   mutate(Month = lubridate::month(dateGame)) %>%
#   filter(Month == 6) %>%
#   select(date = dateGame) %>%
#   mutate(NBA_Finals = 1)
nba_finals <- read_csv("data/nba_finals.csv")

# US Open (Sundays)
us_open <- read_csv("data/us_open.csv") %>%
  mutate(date = lubridate::mdy(date)) %>%
  mutate(USO_Sunday = 1) %>%
  select(-event)

# MLB schedule for teams in WI, CA, and TX
mlb <- read_csv('data/mlb_games.csv')

# Horse racing (triple crown)
horse_race <- read_csv("data/horse_racing.csv") %>%
  mutate(date = lubridate::mdy(date)) %>%
  mutate(Horse_Race = 1) %>%
  spread(event, Horse_Race, fill = 0)

# Combine football, nba, us open, mlb, and horse racing
ohe_sports <- left_join(football, nba_finals, by = "date") %>%
  left_join(us_open, by = "date") %>%
  left_join(mlb, by = "date") %>%
  left_join(horse_race, by = "date") %>%
  replace(is.na(.), 0)

# future sports dates
future_sports_df <- future_calendar %>%
  mutate(NFL_Sun = ifelse(weekday == "Sunday" & (month > 8 | month < 2), 1, 0)) %>%
  mutate(NCAA_Sat = ifelse(weekday == "Saturday" & (month > 8 & month <= 12), 1, 0)) %>%
  select(date, NFL_Sun, NCAA_Sat) %>%
  left_join(nba_finals, by = "date") %>%
  left_join(us_open, by = "date") %>%
  left_join(mlb, by = "date") %>%
  left_join(horse_race, by = "date") %>%
  replace(is.na(.), 0) %>%
  distinct()

range(future_sports_df$date)

################
## Items list ##
################

# separate all items using list
# modeling will use split-apply-combine method
items_list <- sample_df %>%
  select(id, store_item, wm_yr_wk, date, Value) %>%
  inner_join(prices, by = c("store_item", "wm_yr_wk")) %>%
  inner_join(item_ohe, by = "store_item") %>%
  # select(id,
  #        store_item,
  #        # item_id,
  #        # dept_id,
  #        # cat_id,
  #        # store_id,
  #        # state_id,
  #        colnames(item_ohe),
  #        date,
  #        wm_yr_wk,
  #        contains("snap"),
#        sell_price,
#        Value) %>%
split(., .$store_item)

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

  ######################################
  ## Sales data with point indicators ##
  ######################################
  
  # combine daily calendar, sports, prices data
  # no lead/lag indicators yet
  point_indicators <- df %>%
    left_join(ohe_calendar, by = "date") %>%
    left_join(ohe_sports, by = "date") %>%
    #filter(date <= "2016-06-21") %>%
    select(-contains("NBAFinalsEnd"),
           -contains("NBAFinalsStart")) %>%
    drop_na()
  
  lead_ref <- ohe_calendar %>%
    left_join(ohe_sports, by = "date") %>%
    left_join(df, by = "date") %>%
    filter(date <= "2016-05-24", date >= "2016-05-23") %>%
    select(-contains("NBAFinalsEnd"),
           -contains("NBAFinalsStart")) %>%
    select(colnames(point_indicators))
  
  #################################
  ## Calendar leading indicators ##
  #################################
  
  # calendar events that are known in advance
  # variables: holidays, sports
  calendar_lead_vars <- point_indicators %>%
    select(date, 
           contains('event'),
           contains("MLB"),
           NFL_Sun,
           NCAA_Sat,
           NBA_Finals,
           USO_Sunday,
           contains("Horse")) %>%
    distinct() 
  
  calendar_lead_vars <- calendar_lead_vars %>%
    bind_rows(lead_ref %>% select(colnames(calendar_lead_vars)))
  
  # create leading indicators up to 2 days
  calendar_lead <- map(seq(1, 2, 1), function(val) {
    lead_val <- function(x) lead(x, val)
    calendar_lead_vars %>%
      select(-date) %>%
      mutate_all(list(lead_val)) %>%
      setNames(., nm = paste(names(.), "Lead", val, sep = "_"))
  }) %>%
    bind_cols(calendar_lead_vars, .) %>%
    drop_na()
  
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
  
  
  # aws.s3::s3write_using(
  #   x = train,
  #   FUN = write_csv,
  #   object = train_file_name,
  #   bucket = "abn-distro"
  # )
  # 
  # aws.s3::s3write_using(
  #   x = eval,
  #   FUN = write_csv,
  #   object = eval_file_name,
  #   bucket = "abn-distro"
  # )
  #  
  # aws.s3::s3write_using(
  #   x = eval_meta,
  #   FUN = write_csv,
  #   object = eval_meta_file_name,
  #   bucket = "abn-distro"
  # )
  
  #Sys.sleep(2)
  
  tt <- aws.s3::get_bucket_df(
    bucket = "abn-distro",
    prefix = "m5_store_items/eval/",
    max = Inf
  )

  dim(tt)
  
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
# clusterExport(cl, c("ohe_calendar",
#                     "future_calendar",
#                     "ohe_sports",
#                     "future_sports_df",
#                     "future_prices"))

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
