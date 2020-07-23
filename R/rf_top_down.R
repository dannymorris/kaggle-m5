
# Process and methodology:
#  - Create aggregated data for levels 1-5
#  - Import bottom level data (rf/gbm ensemble, no multiplier)
#  - Aggregate bottom level data
#  - Align aggregated bottom level with levels 1-5 using multiplers chosen 
#    by minimizing RMSE
#  - Apply multipliers to bottom levels forecasts
#  - Ensemble

##############
## Packages ##
##############

library(tidyverse)
library(data.table)
library(ranger)
library(lubridate)
library(prophet)
library(plotly)

# remove object and run garbage collect
run_gc <- function(x) {
  rm(x)
  gc()
}

write_submission <- function(eval_sub, output_fname) {
  val_sub <- eval_sub %>%
    mutate(id = str_replace(id, "evaluation", "validation"))
  all_sub <- bind_rows(val_sub, eval_sub)
  sample_sub <- read_csv("data/sample_submission.csv") %>%
    select(id)
  final_sub <- inner_join(sample_sub, all_sub, by = "id")
  write_csv(final_sub, output_fname)
}

##########
## Data ##
##########

# calendar ref
calendar <- read_csv("data/calendar.csv")

# sales
sample_df <- fread("data/sales_train_evaluation.csv") %>%
  gather(Date, Value, -id:-state_id) %>%
  inner_join(calendar, by = c("Date" = "d")) %>%
  filter(date >= "2013-01-01") %>%
  select(id, item_id, dept_id, cat_id, store_id, state_id,
         Date, wm_yr_wk, Value, date, weekday, month)

# onehot encoded holidays
ohe_holidays <- calendar %>%
  select(date, contains("event")) %>%
  gather(key, event, -date) %>%
  select(-key) %>%
  mutate(Ind = 1) %>%
  mutate(event = str_replace_all(event, " ", "")) %>%
  mutate(event = str_replace_all(event, "[^[:alnum:]]", "")) %>%
  mutate(event = paste("event", event, sep = "_")) %>%
  distinct() %>%
  spread(event, Ind, fill = 0) 

# holiday leading indicators (1 and 2 days)
holiday_lead_span <- seq(1, 2, 1)

holiday_leading <- map(holiday_lead_span, function(val) {
  lead_val <- function(x) lead(x, val)
  ohe_holidays %>%
    select(-date) %>%
    mutate_all(list(lead_val)) %>%
    setNames(., nm = paste(names(.), "Lead", val, sep = "_")) 
}) %>%
  bind_cols() %>%
  mutate(date = ohe_holidays$date)

holidays_features <- inner_join(ohe_holidays, holiday_leading, by = "date")

# NFL and NCAA football
football <- calendar %>%
  select(date, weekday, month) %>%
  distinct() %>%
  mutate(NFL_Sun = ifelse(weekday == "Sunday" & (month > 8 | month < 2), 1, 0)) %>%
  mutate(NCAA_Sat = ifelse(weekday == "Saturday" & (month > 8 & month <= 12), 1, 0)) %>%
  select(date, NFL_Sun, NCAA_Sat) %>%
  distinct()

# NBA finals
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

# sports leading indicators
sports_lead_span <- seq(1, 1, 1)

sports_lead <- map(sports_lead_span, function(val) {
  lead_val <- function(x) lead(x, val)
  ohe_sports %>%
    select(-date) %>%
    mutate_all(list(lead_val)) %>%
    setNames(., nm = paste(names(.), "Lead", val, sep = "_")) 
}) %>%
  bind_cols() %>%
  mutate(date = ohe_sports$date)

# sports df with point and leading indicators
sports_features <- inner_join(ohe_sports, sports_lead, by = "date")

# SNAP indicator
snap <- calendar %>%
  select(date, contains("snap"))

# Date features
date_enc <- calendar %>%
  mutate(month = month(date),
         weekday = wday(date),
         monthday = mday(date),
         year = year(date),
         weekmonth = stringi::stri_datetime_fields(date)$WeekOfMonth) %>%
  select(date, year, month, weekmonth, weekday, monthday)


##########################################
## Aggregate sales by department, store ##
##########################################

# by state
by_state <- sample_df %>%
  group_by(date, state_id) %>%
  summarise(Value = sum(Value)) %>%
  ungroup()

# by store
by_store <- sample_df %>%
  group_by(date, store_id) %>%
  summarise(Value = sum(Value)) %>%
  ungroup()

# by dept
by_dept <- sample_df %>%
  group_by(date, dept_id) %>%
  summarise(Value = sum(Value)) %>%
  ungroup()

# by cat
by_cat <- sample_df %>%
  group_by(date, cat_id) %>%
  summarise(Value = sum(Value)) %>%
  ungroup()

# by store, dept
by_store_dept <- sample_df %>%
  group_by(date, store_id, dept_id) %>%
  summarise(Value = sum(Value)) %>%
  ungroup()

#########################
## Top level alignment ##
#########################

# top level
top_level <- sample_df %>%
  group_by(date) %>%
  summarise(Value = sum(Value)) %>%
  ungroup()

# lag-28 sales features (mean, sd)
roll_lags <- top_level %>%
  arrange(date) %>%
  mutate(roll_sd_28 = zoo::rollapply(Value, 28, sd, fill=NA)) %>%
  mutate(roll_sd_28 = lag(roll_sd_28, 28)) %>%
  mutate(roll_mean_28 = zoo::rollapply(Value, 28, mean, fill=NA)) %>%
  mutate(roll_mean_28 = lag(roll_mean_28, 28)) %>%
  drop_na() %>%
  ungroup()

# data ready for RF
model_df <- inner_join(roll_lags, snap, by = "date") %>%
  left_join(holidays_features, by = "date") %>%
  left_join(sports_features, by = "date") %>%
  left_join(date_enc, by = "date") %>%
  replace(is.na(.), 0) %>%
  mutate_if(is.character, as.factor)

# fit RF
rf <- ranger(
  Value ~ .,
  data = model_df %>% select(-date),
  num.trees = 1000,
  mtry = ncol(model_df)/2
)

prophet_df <- model_df %>% select(ds = date, y = Value)
prophet_spec <- prophet(weekly.seasonality = T,
                       yearly.seasonality = T) %>%
  add_seasonality()

# fitted values
fitted_vals <- model_df %>% 
  mutate(fcast = predict(rf, model_df)$predictions) %>%
  select(date, Value, fcast)

plot_ly(data = fitted_vals, x = ~date) %>%
  add_lines(y = ~Value, name = "Actual") %>%
  add_lines(y = ~fcast, name = "Forecast")

# future data
future_df <- model_df %>%
  select(date, Value) %>%
  filter(date >= "2016-03-25") %>%
  arrange(date) %>%
  mutate(roll_sd_28 = zoo::rollapply(Value, 28, sd, fill=NA)) %>%
  mutate(roll_mean_28 = zoo::rollapply(Value, 28, mean, fill=NA)) %>%
  ungroup() %>%
  filter(date >= "2016-04-25") %>%
  mutate(date = date + days(28)) %>%
  inner_join(snap, by = "date") %>%
  left_join(holidays_features, by = "date") %>%
  left_join(sports_features, by = "date") %>%
  left_join(date_enc, by = "date") %>%
  replace(is.na(.), 0) 

# top level forecast
fcast <- future_df %>%
  mutate(fcast = predict(rf, data = .)$predictions) %>%
  mutate(d = paste("F", 1:28, sep = "")) %>%
  select(d, fcast)

# RF bottom-level
ml_bu <- fread("rf_bu_mini_gbm.csv") %>%
  filter(str_detect(id, "evaluation")) %>%
  gather(date, Value, -id) 

ml_agg <- ml_bu %>%
  group_by(date) %>%
  summarise(
    bu = sum(Value)
  ) %>%
  inner_join(fcast, by = c("date" = "d")) 

fcast_dates <- paste("F", 1:28, sep = "")

multipliers <- seq(0.96, 1.3, by = .01)

mult_rmse <- map(multipliers, function(mult) {
  ml_bu %>%
    inner_join(ml_agg, by = "date") %>%
    mutate(final_fcast = fcast*mult) %>%
    yardstick::rmse(truth = Value, estimate = final_fcast) %>%
    mutate(mult = mult)
}) %>%
  bind_rows() %>%
  arrange(.estimate)

eval_pred <- ml_bu %>%
  mutate(Value = Value*.96) %>%
  spread(date, Value) %>%
  select(id, fcast_dates)

write_submission(eval_pred, "ens_bu_rf_top_mult96.csv")

############
## Models ##
############

roll_lags <- by_store_dept %>%
  arrange(store_id, dept_id, date) %>%
  group_by(store_id, dept_id) %>%
  mutate(RollSd30 = zoo::rollapply(Value, 28, sd, fill=NA)) %>%
  mutate(RollSd30 = lag(RollSd30, 28)) %>%
  mutate(RollMean30 = zoo::rollapply(Value, 28, mean, fill=NA)) %>%
  mutate(RollMean30 = lag(RollMean30, 28)) %>%
  drop_na() %>%
  ungroup()

model_df <- inner_join(roll_lags, snap, by = "date") %>%
  left_join(holidays_df, by = "date") %>%
  left_join(ohe_sports, by = "date") %>%
  replace(is.na(.), 0) %>%
  mutate_if(is.character, as.factor)

rf <- ranger(
  Value ~ .,
  data = model_df %>% select(-date),
  num.trees = 1000,
  mtry = ncol(model_df)/2
)

future_df <- model_df %>%
  select(date, dept_id, store_id, state_id, Value) %>%
  filter(date >= "2016-03-25") %>%
  arrange(store_id, dept_id, date) %>%
  group_by(store_id, dept_id) %>%
  mutate(RollSd30 = zoo::rollapply(Value, 28, sd, fill=NA)) %>%
  mutate(RollMean30 = zoo::rollapply(Value, 28, mean, fill=NA))  %>%
  ungroup() %>%
  filter(date >= "2016-04-25") %>%
  mutate(date = date + days(28)) %>%
  inner_join(snap, by = "date") %>%
  left_join(holidays_df, by = "date") %>%
  left_join(ohe_sports, by = "date") %>%
  replace(is.na(.), 0) %>%
  mutate(Pred = predict(rf, data = .)$predictions) %>%
  select(date, dept_id, store_id, Pred)


################
## Submission ##
###############

date_ids <- paste("F", 1:28, sep = "")

eval_sub <- props_final %>%
  inner_join(future_df %>%
               group_by(dept_id, store_id) %>%
               mutate(Idx = seq_along(date)) %>%
               ungroup(), 
             by = c("dept_id", "store_id", "Idx")) %>%
  mutate(Pred = Pred*Prop_Fcast) %>%
  select(id, Idx, Pred) %>%
  spread(Idx, Pred) %>%
  setNames(., c("id", date_ids))

val_sub <- eval_sub %>%
  mutate(id = str_replace(id, "evaluation", "validation"))

all_sub <- bind_rows(val_sub, eval_sub)

sample_sub <- read_csv("data/sample_submission.csv") %>%
  select(id)

final_sub <- inner_join(sample_sub, all_sub, by = "id")

write_csv(final_sub, "top_down_rf.csv")
