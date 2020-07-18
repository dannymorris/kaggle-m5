

## Run this on EC2 using RStudio AMI


library(tidyverse)
library(recipes) 
library(lubridate)
library(zoo)
library(h2o)
library(parallel)
library(furrr)
library(data.table)

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
rm(prices)
gc()

ports <- rep(1:200, length(items_list))[1:length(items_list)]

items_list <- map2(items_list, ports, function(df, port) {
  df[, "port"] <- port
  return(df)
})

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
                    "future_prices",
                    "ports"))

start_time <- Sys.time()

#df <- items_list[[31]]

models <- parallel::parLapply(cl, items_list, function(df) {
  
  library(tidyverse)
  library(lubridate)
  library(zoo)
  #library(h2o)
  # 

  ####################
  # Item indicators #
  ####################
  
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
           id:state_id,
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
  
  # h2o.init(nthreads = 1, port = 54321+unique(df$port))
  # h2o.stopLogging()
  # h2o.no_progress()
  
  out <- bind_rows(
    model_tbl %>% mutate(Split = "Train"),
    future_model_tbl %>% mutate(Split = "Eval")
  ) 
    #h2o::as.h2o()
    #data.table::as.data.table()
  
  # h2o.shutdown()
  
  rm(calendar_fixed)
  rm(calendar_lag)
  rm(point_indicators)
  rm(model_tbl)
  rm(train)
  
  return(out)
})

dt1 <- bind_rows(models)
dt2 <- data.table::rbindlist(models[10001:20000])
dt3 <- data.table::rbindlist(models[20001:30490])


#model_list <- data.table::rbindlist(models)
#h2o_df <- h2o.rbind(models)

rm(models)
gc()

h2o.shutdown()
h2o.init(nthreads = -1, min_mem_size = "280G")

model_df <- h2o::as.h2o(dt1)

#rm(items_list)

end_time <- Sys.time()

end_time - start_time


#########
## H2O ##
#########

model_df <- h2o.rbind(models)

train <- model_df[model_df$Split == "Train",]
train[, c(2,3,4,5,6,7,8)] <- as.factor(train[, c(2,3,4,5,6,7,8)])

eval <- model_df[model_df$Split == "Eval",]
eval[, c(2,3,4,5,6,7,8)] <- as.factor(eval[, c(2,3,4,5,6,7,8)])

rm(dt1)
gc()

#h2o.init(nthreads = 46)

ignore_features <- which(colnames(train) %in% c("date", "Value", "id", "store_item"))
features <- colnames(train)[-ignore_features]
y <- "Value"

fit <- h2o.gbm(x = features,
               y = y,
               training_frame = train,
               ntrees = 2000,
               learn_rate = 0.05,
               learn_rate_annealing = 0.99,
               col_sample_rate = 0.67,
               sample_rate = 0.67,
               stopping_rounds = 0,
               categorical_encoding = "Enum",
               distribution = "tweedie")

# function to return vector of predictions from H2O fitted models.
predict_h2o <- function(m, test_df) {
  test_df %>%
    as.h2o() %>%
    predict(m, newdata = .) %>%
    as.vector()
}

fcast <- predict_h2o(fit, test_df = eval)

eval[, "Value"] <- fcast


################
## Submission ##
################

eval_pred <- eval %>%
  select(date, id, Value) %>%
  spread(date, Value) %>%
  setNames(., nm = c("id", paste("F", 1:28, sep = "")))

val_pred <- eval_pred %>%
  mutate(id = str_replace_all(id, "evaluation", "validation"))

sample_sub <- read_csv("data/sample_submission.csv") %>%
  select(id)

final_sub <- bind_rows(val_pred, eval_pred) %>%
  inner_join(sample_sub, ., by = "id")

bind_rows(final_sub) %>%
  write_csv("eval_submission.csv")
