install.packages("aws.s3", repos = "https://cloud.R-project.org")

library(tidyverse)
library(aws.s3)
library(parallel)

sample_sub <- read_csv("sample_submission.csv")

Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           ",
           "AWS_DEFAULT_REGION" = "us-east-1")

#####################################
# Get dataframe of object locations #
#####################################

bucket <- "abn-distro"
prefix <- "m5_store_items/eval_inference/"

pred_objects <- get_bucket_df(
  bucket = bucket,
  prefix = prefix,
  max = Inf
)

###############################
# Format object/product names #
###############################

object_names <- pred_objects$Key

product_names <- object_names %>%
  str_split(., "/") %>%
  map(., function(x) x[3]) %>%
  flatten_chr() %>%
  str_replace(., ".csv.out", "")

###################
# Get predictions #
###################

# make cluster
n_cores <- parallel::detectCores()-1
n_cores
cl <- makeCluster(n_cores)
clusterExport(cl, c("object_names", "product_names"))

# index files
item_seq <- as.list(seq(1, length(object_names), 1))

# get predictions
pred_tbls <- parallel::parLapply(cl, item_seq, function(i) {
  
  library(dplyr)
  library(tidyr)
  library(aws.s3)
  library(readr)
  
  bucket <- "abn-distro"
  
  s3read_using(
    FUN = read_csv,
    object = object_names[i],
    bucket = bucket,
    col_names = F
  ) %>%
    mutate(id = product_names[i]) %>%
    mutate(d = row_number()) %>%
    spread(d, X1) %>%
    setNames(., nm = c("id", paste("F", 1:28, sep = "")))
})

# stop cluster
stopCluster(cl)

# convert eval predictions into dataframe
eval_pred <- bind_rows(pred_tbls)

# create val predictions
val_pred <- eval_pred %>%
  mutate(id = str_replace(id, "evaluation", "validation"))

all_pred <- bind_rows(val_pred, eval_pred)

final_sub <- sample_sub %>%
  select(id) %>%
  inner_join(all_pred, by = "id")

write_csv(final_sub, "final_submission.csv")
