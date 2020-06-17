## Final submission

library(tidyverse)

eval <- read_csv("eval_submission.csv") %>%
  mutate(id = str_replace_all(id, "validation", "evaluation"))

val <- read_csv("val_submission.csv")

final_submission <- bind_rows(val, eval)

sample <- read_csv("R/sample_submission.csv") %>%
  select(id)

final_submission <- inner_join(sample, final_submission, by = "id")

write_csv(final_submission, "final_submission.csv")
