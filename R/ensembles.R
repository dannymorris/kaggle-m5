library(tidyverse)

gbm <- read_csv("minified_h2o_gbm_poisson_1500.csv") %>% gather(key, value, -id)
rf <- read_csv("eval_submission (1).csv") %>% gather(key, value, -id)

date_ids <- paste("F", 1:28, sep = "")

ens <- inner_join(gbm, rf, by = c("id", "key")) %>%
  mutate(Ens = (value.x + value.y)/2) %>%
  select(id, key, Ens) %>%
  mutate(key = str_replace(key, "F", "")) %>%
  mutate(key = as.integer(key)) %>%
  spread(key, Ens) %>%
  setNames(., c("id", date_ids))

sample_sub <- read_csv("data/sample_submission.csv") %>%
  select(id)

final_sub <- inner_join(sample_sub, ens, by = "id")

mult <- function(x,y) x*y

final_sub_mult97 <- final_sub %>%
  map_at(vars(F1:F28), function(x) mult(x, y=0.97)) %>%
  bind_cols()

final_sub_mult99 <- final_sub %>%
  map_at(vars(F1:F28), function(x) mult(x, y=0.99)) %>%
  bind_cols()

final_sub_mult101 <- final_sub %>%
  map_at(vars(F1:F28), function(x) mult(x, y=1.01)) %>%
  bind_cols()

final_sub_mult103 <- final_sub %>%
  map_at(vars(F1:F28), function(x) mult(x, y=1.03)) %>%
  bind_cols()

write_csv(final_sub, "rf_bu_mini_gbm.csv")
write_csv(final_sub_mult97, "rf_bu_mini_gbm_mult97.csv")
write_csv(final_sub_mult99, "rf_bu_mini_gbm_mult99.csv")
write_csv(final_sub_mult101, "rf_bu_mini_gbm_mult101.csv")
write_csv(final_sub_mult103, "rf_bu_mini_gbm_mult103.csv")
