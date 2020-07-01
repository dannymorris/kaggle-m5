

## Run this on EC2 using RStudio AMI


library(tidyverse)
library(recipes) 
#library(mlbgameday)
#library(nbastatR)
library(lubridate)
library(zoo)
library(ranger)
#library(h2o)
library(parallel)

# calendar reference
calendar <- read_csv("data/calendar.csv")

# prices
prices <- read_csv("data/sell_prices.csv") %>%
  mutate(store_item = paste(item_id, store_id, sep = "_")) %>%
  select(-item_id,
         -store_id)

# training set
sample_df <- read_csv("data/sales_train_evaluation.csv") %>%
  mutate(store_item = paste(item_id, store_id, sep = "_")) %>%
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

##############
## Calendar ##
##############

# one-hot enconde date components
# weekday, weekmonth, month, year
ohe_date <- calendar %>%
  mutate(weekmonth = stringi::stri_datetime_fields(date)$WeekOfMonth) %>%
  select(weekday, weekmonth, month, year) %>%
  mutate_at(vars(weekmonth, month, year), as.factor) %>%
  recipes::recipe(~., data = .) %>%
  step_dummy(all_predictors(), one_hot = T) %>%
  prep() %>%
  juice()

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
  select(-contains("event"), -wday,  -year) %>%
  bind_cols(ohe_date) %>%
  left_join(ohe_holidays, by = "date") %>%
  replace(is.na(.), 0)

# future calendar dates
future_calendar <- ohe_calendar %>%
  filter(date >= "2016-04-25", date <= "2016-05-22")

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
  inner_join(prices, by = c("store_item", "wm_yr_wk")) %>%
  select(id,
         store_item,
         item_id,
         dept_id,
         cat_id,
         store_id,
         state_id,
         date,
         wm_yr_wk,
         contains("snap"),
         sell_price,
         Value) %>%
  split(., .$store_item)

rm(sample_df)
gc()

###########
## Model ##
###########

n_cores <- parallel::detectCores()-1
n_cores
cl <- makeCluster(n_cores)
clusterExport(cl, c("ohe_calendar",
                    "future_calendar",
                    "ohe_sports",
                    "future_sports_df",
                    "future_prices"))

start_time <- Sys.time()

#df <- items_list[[31]]

models <- parallel::parLapply(cl, items_list, function(df) {
  
  library(tidyverse)
  library(lubridate)
  library(zoo)
  library(ranger)
  
  # item ids
  item_state <- unique(df$state_id)
  item_ID <- unique(df$item_id)
  store_ID <- unique(df$store_id)
  
  ######################################
  ## Sales data with point indicators ##
  ######################################
  
  # combine daily calendar, sports, prices data
  # no lead/lag indicators yet
  point_indicators <- df %>%
    mutate(released = ifelse(cumsum(Value > 0), 1, 0)) %>%
    left_join(ohe_calendar, by = "date") %>%
    left_join(ohe_sports, by = "date") %>%
    filter(date <= "2016-05-24") %>%
    select(-contains("NBAFinalsEnd"),
           -contains("NBAFinalsStart")) %>%
    drop_na()
  
  lead_ref <- ohe_calendar %>%
    left_join(ohe_sports, by = "date") %>%
    left_join(df, by = "date") %>%
    mutate(released = ifelse(cumsum(Value > 0), 1, 0)) %>%
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
           #contains("snap"),
           contains("weekday"),
           contains("month"),
           contains("year"),
           contains("weekmonth"),
           -contains("event"),
           -weekday,
           -month) %>%
    distinct()
  
  # future fixed
  future_fixed_tbls <- future_calendar %>%
    select(date,
           contains("snap"),
           contains("weekday"),
           contains("month"),
           contains("year"),
           contains("weekmonth"),
           -contains("event"),
           -weekday,
           -month)
  
  ###############
  # subset SNAP #
  ###############
  
  # get state SNAP indicators
  item_snap <- point_indicators %>%
    select(date, contains("snap")) %>%
    select(date, contains(item_state))
  
  ##############
  # subset MLB #
  ##############
  
  # get MLB games in item state
  item_mlb_state <- calendar_lead %>%
    select(date, contains("MLB")) %>%
    select(date, contains(item_state))
  
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
           sell_price_Lag_1,
           promo,
           hike)
  
  #########################
  # Rolling sales metrics #
  #########################
  
  calc_trend <- function(x) {
    lm(x ~ seq_along(x))$coefficients[2]
  }
  
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
    mutate(Roll_Lm_28 = zoo::rollapply(Value, 28, calc_trend, align = 'right', fill = NA)) %>%
    mutate(Roll_Lm_14 = zoo::rollapply(Value, 14, calc_trend, align = 'right', fill = NA)) %>%
    mutate(Roll_Lm_7 = zoo::rollapply(Value, 7, calc_trend, align = 'right', fill = NA)) %>%
    mutate(Roll_Lm_28_Shift = lag(Roll_Lm_28, 28)) %>%
    mutate(Roll_Lm_14_Shift = lag(Roll_Lm_14, 14)) %>%
    mutate(Roll_Lm_7_Shift = lag(Roll_Lm_7, 7)) %>%
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
           contains("year"),
           contains("month"),
           contains("weekday"),
           contains("weekmonth"),
           contains("snap"),
           contains("NFL"),
           contains("NBA"),
           contains("NCAA"),
           contains("USO"),
           contains("MLB"),
           contains("Horse"),
           contains("event"),
           contains("Roll"),
           sell_price,
           promo,
           hike,
           Value) 
  
  #################
  # Training data #
  #################
  
  eval_months <- model_tbl %>%
    filter(month_X4==1 | month_X5==1 | month_X6 == 1)
  
  date_sample <- model_tbl %>%
    filter(date >= "2014-01-01") %>%
    anti_join(eval_months, by = "date")
  
  train <- bind_rows(date_sample, eval_months)
  
  range(train$date)
  
  #################
  # Random Forest #
  #################
  
  rf <- ranger::ranger(
    formula = Value ~ .,
    data = train %>% select(-date),
    importance = 'impurity',
    #mtry = round(ncol(train)*.33),
    num.trees = 750
  )
  
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
    mutate(Roll_Lm_28_Shift = zoo::rollapply(Value, 28, calc_trend, align = 'right', fill = NA)) %>%
    mutate(Roll_Lm_14_Shift = zoo::rollapply(Value, 14, calc_trend, align = 'right', fill = NA)) %>%
    mutate(Roll_Lm_7_Shift = zoo::rollapply(Value, 7, calc_trend, align = 'right', fill = NA)) %>%
    select(date, Roll_Mean_28_Shift:Roll_Lm_7_Shift) %>%
    drop_na() %>%
    mutate(date = date + days(28))
  
  # future modeling table
  future_model_tbl <- future_calendar %>%
    select(date) %>%
    inner_join(future_item_prices %>% select(date, hike, promo), by = "date") %>%
    inner_join(future_rolling, by = "date") %>%
    inner_join(future_lead_tbls, by = "date") %>%
    inner_join(future_lag_tbls, by = "date") %>%
    inner_join(future_fixed_tbls, by = "date") %>%
    select(date, everything()) %>%
    mutate(Zero_Demand = 0) %>%
    replace(is.na(.), 0)
  
  rf_fcast <- predict(rf, data = future_model_tbl)$predictions
  
  out <- future_model_tbl %>%
    select(date) %>%
    mutate(Prediction = rf_fcast) %>%
    mutate(Prediction = ifelse(Prediction < 0, 0, Prediction)) %>%
    mutate(id = paste(item_ID, store_ID, "evaluation", sep = "_")) %>%
    inner_join(future_calendar %>%
                 select(date, d),
               by = "date") %>%
    select(-date) %>%
    spread(d, Prediction) %>%
    setNames(., nm = c("id", paste("F", 1:28, sep = "")))
})

end_time <- Sys.time()

end_time - start_time

################
## Submission ##
################

eval_pred <- map(models, function(model) {
  model[['predictions']]
}) %>%
  bind_rows()

val_pred <- eval_pred %>%
  mutate(id = str_replace_all(id, "evaluation", "validation"))

sample_sub <- read_csv("sample_submission.csv") %>%
  select(id)

final_sub <- bind_rows(val_pred, eval_pred) %>%
  inner_join(sample_sub, ., by = "id")

bind_rows(final_sub) %>%
  write_csv("eval_submission.csv")
