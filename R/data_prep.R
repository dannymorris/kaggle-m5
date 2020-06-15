
library(tidyverse)
library(lubridate)
library(recipes)

##############
## Calendar ##
##############

# calendar reference
calendar <- read_csv("data/calendar.csv")

ohe <- calendar %>%
  select(contains("event"), weekday, month, year) %>%
  mutate_at(vars(month, year), as.factor) %>%
  replace(is.na(.), "Normal") %>%
  recipes::recipe(~., data = .) %>%
  step_dummy(all_predictors(), one_hot = T) %>%
  prep() %>%
  juice()

clean_calendar <- calendar %>%
  select(-contains("event")) %>%
  bind_cols(ohe)

future_calendar <- clean_calendar %>%
  filter(date >= "2016-05-23")


####################
## Selling prices ##

# prices
sell_price <- read_csv("sell_prices.csv")

# training set
train <- read_csv("sales_train_validation.csv") %>%
  gather(Date, Value, -id:-state_id) %>%
  inner_join(calendar, by = c("Date" = "d")) %>%
  left_join(sell_price, by = c("store_id", "item_id", "wm_yr_wk")) %>%
  mutate(Split = "Train")










