
library(tidyverse)
library(mlbgameday)
library(nbastatR)
library(lubridate)
library(pdfetch)
library(ranger)

##########
## Data ##
##########

model_df <- read_csv("data/model_df.csv")

# sample submission
sample_sub <- read_csv("data/sample_submission.csv")

#################
## Sample data ##
#################

# 5 items for development purposes

sample_items <- model_df %>%
  group_by(store_id, item_id) %>%
  summarise(
    mean_val = mean(Value),
    mean_price = mean(sell_price, na.rm = T)
  ) %>%
  arrange(desc(mean_val)) %>%
  ungroup() %>%
  dplyr::slice(1:10) %>%
  select(store_id, item_id)

sample_df <- model_df %>%
  inner_join(sample_items, by = c("store_id", "item_id"))  %>%
  select(id:date,
         weekday,
         month,
         Split)

##############
## Calendar ##
##############

# calendar reference
calendar <- read_csv("data/calendar.csv")

ohe_date <- calendar %>%
  mutate(weekmonth = stringi::stri_datetime_fields(date)$WeekOfMonth) %>%
  select(weekday, weekmonth, month, year) %>%
  mutate_at(vars(weekmonth, month, year), as.factor) %>%
  recipes::recipe(~., data = .) %>%
  step_dummy(all_predictors(), one_hot = T) %>%
  prep() %>%
  juice()

ohe_events <- calendar %>%
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

clean_calendar <- calendar %>%
  select(-contains("event"), -wday,  -year) %>%
  bind_cols(ohe_date) %>%
  left_join(ohe_events, by = "date") %>%
  replace(is.na(.), 0)

##########
# Prices #
##########

prices <- read_csv("data/sell_prices.csv")

#################
## Sports data ##
#################

# NFL and College football
football <- sample_df %>%
  mutate(NFL_Sun = ifelse(weekday == "Sunday" & (month > 8 | month < 2), 1, 0)) %>%
  mutate(NCAA_Sat = ifelse(weekday == "Saturday" & (month > 8 & month <= 12), 1, 0)) %>%
  select(date, NFL_Sun, NCAA_Sat) %>%
  distinct()

# NBA Finals schedule
nba_finals <- nbastatR::seasons_schedule(2011:2016, season_types = 'Playoffs') %>%
  mutate(Month = lubridate::month(dateGame)) %>%
  filter(Month == 6) %>%
  select(date = dateGame) %>%
  mutate(NBA_Finals = 1)

# US Open (sundays)
us_open <- read_csv("data/us_open.csv") %>%
  mutate(date = lubridate::mdy(date)) %>%
  mutate(USO_Sunday = 1) %>%
  select(-event)

# MLB
teams <- tibble::tribble(
  ~team, ~state_id,
  "Brewers", "WI",
  "Angels", "CA",
  "Padres", "CA",
  "Athletics", "CA",
  "Giants", "CA",
  "Dodgers", "CA",
  "Astros", "TX",
  "Rangers", "TX"
)

mlb <- mlbgameday::game_ids %>% 
  as_tibble() %>%
  mutate(date = as.Date(date_dt)) %>%
  select(date, home_team_name, away_team_name) %>%
  gather(home_away, team, -date) %>%
  inner_join(teams, by = "team") %>%
  filter(date >= "2011-01-01", date <= "2016-07-01") %>%
  group_by(date, state_id, home_away) %>%
  count() %>%
  mutate(home_away_state = paste("MLB", substr(home_away, 1, 4), state_id, sep = "_")) %>%
  ungroup() %>%
  select(-home_away, -state_id) %>%
  spread(home_away_state, n) %>%
  replace(is.na(.), 0)

# Horse racing
horse_race <- read_csv("data/horse_racing.csv") %>%
  mutate(date = lubridate::mdy(date)) %>%
  mutate(Horse_Race = 1) %>%
  spread(event, Horse_Race, fill = 0)

sports_df <- left_join(football, nba_finals, by = "date") %>%
  left_join(us_open, by = "date") %>%
  left_join(mlb, by = "date") %>%
  left_join(horse_race, by = "date") %>%
  replace(is.na(.), 0)


######################################
## Sales data with point indicators ##
######################################

sales_df <- sample_df %>%
  select(-weekday, -month) %>%
  inner_join(clean_calendar, by = "date") %>%
  inner_join(sports_df, by = "date") %>%
  left_join(prices, by = c("store_id", "item_id", "wm_yr_wk")) %>%
  select(-contains("NBAFinalsEnd"),
         -contains("NBAFinalsStart")) %>%
  drop_na()

#################################
## Calendar leading indicators ##
#################################

calendar_lead_vars <- sales_df %>%
  select(date, 
         contains('event'),
         contains("MLB"),
         NFL_Sun,
         NCAA_Sat,
         NBA_Finals,
         USO_Sunday,
         contains("Horse")) %>%
  distinct()

calendar_lead <- map(seq(1, 2, 1), function(val) {
  lead_val <- function(x) lead(x, val)
  calendar_lead_vars %>%
    select(-date) %>%
    mutate_all(list(lead_val)) %>%
    setNames(., nm = paste(names(.), "Lead", val, sep = "_"))
}) %>%
  bind_cols(calendar_lead_vars, .)


#################################
## Calendar lagging indicators ##
#################################

calendar_lag <- sales_df %>%
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

calendar_fixed <- sales_df %>%
  select(Split,
         date,
         contains("weekday"),
         contains("month"),
         contains("year"),
         contains("weekmonth"),
         -contains("event"),
         -weekday,
         -month) %>%
  distinct()

################
## Items list ##
################

items_list <- sales_df %>%
  select(id,
         item_id,
         dept_id,
         cat_id,
         store_id,
         state_id,
         date,
         wm_yr_wk,
         d,
         contains("snap"),
         sell_price,
         Value) %>%
  split(., list(.$item_id, .$store_id))

###########
## Model ##
###########

items_list %>%
  map(., function(df) {
    
    #######################################
    # Identify appropriate SNAP indicator #
    #######################################
    
    item_state <- unique(df$state_id)
    
    item_snap <- df %>%
      select(date, contains("snap")) %>%
      select(date, contains(item_state))
    
    item_mlb_state <- calendar_lead %>%
      select(date, contains("MLB")) %>%
      select(date, contains(item_state))
    
    item_prices <- df %>%
      mutate(sell_price_Lag_1 = lag(sell_price, 1)) %>%
      mutate(sell_price_diff = sell_price - lag(sell_price, 1)) %>%
      select(date,
             sell_price_Lag_1)
    
    item_rolling <- df %>%
      select(date, Value) %>%
      mutate(Roll_Median_30 = zoo::rollmedian(Value, 29, align = 'right', fill = NA)) %>%
      mutate(Roll_Median_7 = zoo::rollmedian(Value, 7, align = 'right', fill = NA)) %>%
      mutate(Roll_Median_14 = zoo::rollmedian(Value, 13, align = 'right', fill = NA)) %>%
      mutate(Roll_Median_30_Shift = lag(Roll_Median_30, 29)) %>%
      mutate(Roll_Median_7_Shift = lag(Roll_Median_7, 29)) %>%
      mutate(Roll_Median_14_Shift = lag(Roll_Median_14, 29)) %>%
      mutate(Roll_Sd_30 = zoo::rollapply(Value, 30, sd, align = 'right', fill = NA)) %>%
      mutate(Roll_Sd_7 = zoo::rollapply(Value, 7, sd, align = 'right', fill = NA)) %>%
      mutate(Roll_Sd_14 = zoo::rollapply(Value, 14, sd, align = 'right', fill = NA)) %>%
      mutate(Roll_Sd_30_Shift = lag(Roll_Sd_30, 30)) %>%
      mutate(Roll_Sd_7_Shift = lag(Roll_Sd_7, 30)) %>%
      mutate(Roll_Sd_14_Shift = lag(Roll_Sd_14, 30)) %>%
      select(date, 
             contains("Shift"))
    
    model_tbl <- df %>%
      select(-contains("snap")) %>%
      inner_join(item_prices, by = "date") %>%
      inner_join(item_snap, by = "date") %>%
      inner_join(item_rolling, by = "date") %>%
      inner_join(calendar_lead, by = "date") %>%
      select(-contains("MLB")) %>%
      inner_join(item_mlb_state, by = "date") %>%
      inner_join(calendar_lag, by = "date") %>%
      inner_join(calendar_fixed, by = "date") %>%
      mutate(Zero_Demand = ifelse(Value == 0, 1, 0))
    
    ############################
    # Train and holdout splits #
    ############################
    
    train <- model_tbl %>% 
      filter(Split == "Train") %>%
      select(date, Split, Value, everything()) %>%
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
             Value,
             Zero_Demand)
    
    holdout <- model_tbl %>% 
      filter(Split == "Test") %>%
      select(date, Split, Value, everything()) %>%
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
             Value,
             Zero_Demand)
    
    dim(train)
    dim(holdout)
    
    colnames(train)
    colnames(holdout)
    
    #################
    # Random forest #
    #################
    
    # fit
    rf <- ranger::ranger(
      formula = Value ~ .,
      data = train %>% select(-date),
      importance = 'impurity',
      mtry = ncol(train)-2,
      num.trees = 250
    )
    
    rf
    
    rf_fitted <- rf$predictions
    rf_pred <- predict(rf, data = holdout)$predictions
    
    # var imp
    rf$variable.importance %>% 
      enframe() %>% 
      arrange(desc(value)) %>%
      head(20)
    
    #############
    ## XGBoost ##
    #############
    
    train_mat <- train %>% select(-Value, -date) %>% as.matrix()
    holdout_mat <- holdout %>% select(-Value, -date) %>% as.matrix()
    
    xgb <- xgboost(data = train_mat, 
                   label = as.matrix(train$Value), 
                   eta = 0.1, 
                   nthread = 2, 
                   nrounds = 100, 
                   max.depth = 5,
                   early_stopping_rounds = 10,
                   objective = "reg:squarederror") 
    
    xgb_fitted <- predict(xgb, newdata = train_mat)
    xgb_pred <- predict(xgb, newdata = holdout_mat)
    
    
    ##################
    ## Holdout Error #
    ##################
    
    holdout_pred_act <- tibble(
      rf_pred = rf_pred,
      xgb_pred = xgb_pred,
      Actual = holdout$Value,
      date = holdout$date
    ) %>%
      mutate(rf_sq_Error = (rf_pred - Actual)**2) %>%
      mutate(xgb_sq_Error = (xgb_pred - Actual)**2)
    
    holdout_pred_act %>%
      ggplot(aes(x = date)) +
      geom_line(aes(y = Actual)) +
      geom_line(aes(y = rf_pred), color = 'blue') +
      geom_line(aes(y = xgb_pred), color = 'green4')
    
    tibble(
      rf_rmse = sqrt(mean(holdout_pred_act$rf_sq_Error)),
      xgb_rmse = sqrt(mean(holdout_pred_act$xgb_sq_Error))
    )
    
    ###############
    # Train Error #
    ###############
    
    train_pred_act <- tibble(
      rf_fitted = rf_fitted,
      xgb_fitted = xgb_fitted,
      Actual = train$Value,
      date = train$date
    ) %>%
      mutate(rf_sq_Error = (rf_fitted - Actual)**2) %>%
      mutate(xgb_sq_Error = (xgb_fitted - Actual)**2)

    plotly::plot_ly(
      data = train_pred_act,
      x = ~date
    ) %>%
      add_lines(y = ~Actual, name = "Actual") %>%
      add_lines(y = ~rf_fitted, name = "RF_Fit") %>%
      add_lines(y = ~xgb_fitted, name = "XGB_Fit")
    
    tibble(
      rf_rmse = sqrt(mean(train_pred_act$rf_sq_Error)),
      xgb_rmse = sqrt(mean(train_pred_act$xgb_sq_Error))
    )
    
    ############
    # Ensemble #
    ############
    
    train_pred_act %>%
      plot_ly(data = ., x = ~rf_fitted, y = ~xgb_fitted)
    
    train_ens_pred_act <- train_pred_act %>%
      mutate(mean_fitted = (rf_fitted + xgb_fitted)/2) %>%
      mutate(mean_fitted = ifelse(mean_fitted < 0, 0, mean_fitted)) %>%
      mutate(mean_sq_Error = (mean_fitted - Actual)**2)
    
    plotly::plot_ly(
      data = train_ens_pred_act,
      x = ~date
    ) %>%
      add_lines(y = ~Actual, name = "Actual") %>%
      add_lines(y = ~rf_fitted, name = "RF_Fit") %>%
      add_lines(y = ~xgb_fitted, name = "XGB_Fit") %>%
      add_lines(y = ~mean_fitted, name = "Mean_Fit")
    
    tibble(
      rf_rmse = sqrt(mean(train_ens_pred_act$rf_sq_Error)),
      xgb_rmse = sqrt(mean(train_ens_pred_act$xgb_sq_Error)),
      ens_rmse = sqrt(mean(train_ens_pred_act$mean_sq_Error))
    )
    
    holdout_ens_pred_act <- holdout_pred_act %>%
      mutate(mean_pred = (rf_pred + xgb_pred)/2) %>%
      mutate(mean_pred = ifelse(mean_pred < 0, 0, mean_pred)) %>%
      mutate(mean_sq_Error = (mean_pred- Actual)**2)
    
    holdout_ens_pred_act %>%
      ggplot(aes(x = date)) +
      geom_line(aes(y = Actual)) +
      geom_line(aes(y = rf_pred), color = 'blue') +
      geom_line(aes(y = xgb_pred), color = 'green4')+
      geom_line(aes(y = mean_pred, color = 'orange')) +
      theme_bw()
    
    tibble(
      rf_rmse = sqrt(mean(holdout_ens_pred_act$rf_sq_Error)),
      xgb_rmse = sqrt(mean(holdout_ens_pred_act$xgb_sq_Error)),
      ens_rmse = sqrt(mean(holdout_ens_pred_act$mean_sq_Error))
    )
    
    ############
    # Forecast #
    ############
    
    # future calendar
    future_calendar <- clean_calendar %>%
      filter(date >= "2016-05-21")
    
    # future prices
    future_prices <- prices %>%
      inner_join(future_calendar %>%
                   select(date, wm_yr_wk), 
                 by = "wm_yr_wk") %>%
      semi_join(df, by = c("store_id", "item_id")) %>%
      select(date, sell_price)
    
    # future sports
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
    
    # future lag
    future_lag_tbls <- inner_join(future_calendar, future_prices, by = "date") %>%
      mutate(sell_price_Lag_1 = lag(sell_price, 1),
             event_Thanksgiving_Lag_1 = lag(event_Thanksgiving, 1),
             event_Christmas_Lag_1 = lag(event_Christmas, 1)) %>%
      select(date, 
             sell_price,
             sell_price_Lag_1,
             event_Thanksgiving_Lag_1,
             event_Christmas_Lag_1)
    
    # future lead
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
    
    # future rolling
    future_rolling <- df %>%
      arrange(date) %>%
      tail(60) %>%
      mutate(Roll_Median_30_Shift = zoo::rollmedian(Value, 29, align = 'right', fill = NA)) %>%
      mutate(Roll_Median_7_Shift = zoo::rollmedian(Value, 7, align = 'right', fill = NA)) %>%
      mutate(Roll_Median_14_Shift = zoo::rollmedian(Value, 13, align = 'right', fill = NA)) %>%
      mutate(Roll_Sd_30_Shift = zoo::rollapply(Value, 30, sd, align = 'right', fill = NA)) %>%
      mutate(Roll_Sd_7_Shift = zoo::rollapply(Value, 7, sd, align = 'right', fill = NA)) %>%
      mutate(Roll_Sd_14_Shift = zoo::rollapply(Value, 14, sd, align = 'right', fill = NA)) %>%
      select(date, 
             Roll_Median_30_Shift:Roll_Sd_14_Shift) %>%
      drop_na() %>%
      mutate(date = date + months(1))
    
    future_model_tbl <- future_calendar %>%
      select(date) %>%
      #inner_join(future_prices, by = "date") %>%
      inner_join(future_rolling, by = "date") %>%
      inner_join(future_lead_tbls, by = "date") %>%
      inner_join(future_lag_tbls, by = "date") %>%
      inner_join(future_fixed_tbls, by = "date") %>%
      select(date, everything()) %>%
      mutate(Zero_Demand = 0) %>%
      replace(is.na(.), 0)
    
    future_xgb_mat <- future_model_tbl %>%
      select(which(colnames(future_model_tbl) %in% colnames(train))) %>%
      select(-date) %>%
      select(all_of(colnames(train_mat))) %>%
      as.matrix()
    
    rf_fcast <- predict(rf, data = future_model_tbl)$predictions
    xgb_fcast <- predict(xgb, newdata = future_xgb_mat)
    ens_fcast <- (rf_fcast + xgb_fcast)/2
    
    item_ID <- unique(df$item_id)
    store_ID <- unique(df$store_id)
    
    out <- future_model_tbl %>%
      select(date) %>%
      mutate(Prediction = ens_fcast) %>%
      mutate(id = paste(item_ID, store_ID, "validation", sep = "_")) %>%
      inner_join(future_calendar %>%
                   select(date, d),
                 by = "date") %>%
      select(-date) %>%
      spread(d, Prediction) %>%
      setNames(., nm = c("id", paste("F", 1:28, sep = "")))
    
    return(out)
    
    
  })

