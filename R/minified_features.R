# This script generates product-level training data.
# Each individual training data set (by product) is uploaded to S3 as a CSV file

## Run this on EC2 using RStudio AMI

Sys.setenv("AWS_ACCESS_KEY_ID" = "AKIAIFR2Z22DZKZFLTIQ",
           "AWS_SECRET_ACCESS_KEY" = "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",
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

replace_na0 <- function(x) {
  ifelse(is.na(x), 0, x)
}

replace_na1 <- function(x) {
  ifelse(is.na(x), 1, x)
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


#################
## Item encode ##
#################

item_enc <- eval_df %>%
  mutate(store_item = paste(item_id, store_id, sep = "_")) %>%
  select(store_item, id, item_id, dept_id, cat_id, store_id, state_id) %>%
  mutate_at(vars(-store_item, -id), as.factor) %>%
  mutate_at(vars(-store_item, -id), label_enc)

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

holidays_df <- bind_cols(holidays_enc, holiday_lead) %>%
  mutate_at(vars(contains("event")), list(replace_na1))

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
sports_ohe <- nba_finals %>%
  left_join(us_open, by = "date") %>%
  left_join(mlb, by = "date") %>%
  left_join(horse_race, by = "date") %>%
  replace(is.na(.), 0) %>%
  left_join(calendar %>% select(date), ., by = "date") %>%
  mutate_at(vars(-date), replace_na0)

sports_lead <- map(seq(1, 1, 1), function(val) {
  lead_val <- function(x) lead(x, val)
  sports_ohe %>%
    select(-date) %>%
    mutate_all(list(lead_val)) %>%
    setNames(., nm = paste(names(.), "Lead", val, sep = "_")) 
}) %>%
  bind_cols()

sports_df <- bind_cols(sports_ohe, sports_lead)

################
## Items list ##
################

# merge encoded features
calendar_enc <- calendar %>%
  select(date, d, wm_yr_wk, contains("snap")) %>%
  inner_join(date_enc, by = "date") %>%
  inner_join(holidays_df, by = "date") %>%
  left_join(sports_df, by = "date")

# sales demand
demand_long <- eval_df %>% 
  select(item_id, store_id, contains('d_')) %>%
  gather(d, Value, -item_id, -store_id)

# model_df
model_df <- prices %>%
  inner_join(calendar %>% select(wm_yr_wk, date, d), by = "wm_yr_wk") %>%
  left_join(demand_long, by = c("store_id", "item_id", "d")) %>%
  select(store_item, date, Value, sell_price) %>%
  left_join(calendar_enc, by = "date") %>%
  left_join(item_enc, by = "store_item") %>%
  select(-store_item, -d, -wm_yr_wk, -id) %>%
  select(date, Value, sell_price, snap_CA:state_id)

train <- model_df %>%
  filter(date < "2016-05-23") %>%
  select(-date)

eval <- model_df %>%
  filter(date >= "2016-05-23") %>%
  select(-date, -Value)

s3write_using(
  x = train,
  FUN = fwrite,
  object = "m5_store_items/train/train.csv",
  bucket = "abn-distro",
  col.names = F,
  row.names = F
)

s3write_using(
  x = eval,
  FUN = fwrite,
  object = "m5_store_items/eval/eval.csv",
  bucket = "abn-distro",
  col.names = F,
  row.names = F
)

