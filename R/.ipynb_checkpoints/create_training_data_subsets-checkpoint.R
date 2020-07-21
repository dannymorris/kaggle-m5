# This script generates training data subsets by store (1) and store-dept (2).
# Subsets are uploaded to S3

# this must be done after uploading product-level data

install.packages("aws.s3", repos = "https://cloud.R-project.org")
install.packages("data.table")

library(tidyverse)
library(aws.s3)
library(parallel)
library(data.table)

Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           "AWS_SECRET_ACCESS_KEY" = "",
           "AWS_DEFAULT_REGION" = "us-east-1")

bucket <- "abn-distro"

train_objects <- get_bucket_df(
  bucket = bucket,
  prefix = "m5_store_items/train/",
  max = Inf
)

train_files <- as.list(train_objects$Key)

# set up parallel cluster
n_cores <- parallel::detectCores()-1
n_cores
cl <- makeCluster(n_cores)

# get product-level data
train_tbls <- parallel::parLapply(cl, train_files, function(i) {
  
  library(aws.s3)
  library(data.table)
  
  bucket <- "abn-distro"
  
  s3read_using(
    FUN = fread,
    object = i,
    bucket = bucket
  ) 
})

# concatenate product-level data
train_df <- data.table::rbindlist(train_tbls)

rm(train_tbls)
gc()


########################
## Subset by store id ##
########################
# split training data by store id
store_splits <- split(train_df, list(train_df$V4))
# list of store ids
store_ids <- as.list(seq(1, length(store_splits), 1))

map(store_ids, function(i) {
  object_name <- paste("m5_store_items/train_stores/store_", i, ".csv", sep="")
  s3write_using(
    x = store_splits[[i]],
    FUN = fwrite,
    sep = ",",
    logical01 = T,
    bucket = bucket,
    object = object_name,
    col.names = F,
    row.names = F
  )
})

rm(store_splits)
gc()

#############################
## Subset by store, dept id ##
#############################
# split training data by store id
store_dept_splits <- split(train_df, list(train_df$V4, train_df$V2))
length(store_dept_splits)
# list of store ids
store_dept_ids <- as.list(seq(1, length(store_dept_splits), 1))

map(store_dept_ids, function(i) {
  object_name <- paste("m5_store_items/train_store_dept/store_dept_", i, ".csv", sep="")
  s3write_using(
    x = store_dept_splits[[i]],
    FUN = fwrite,
    sep = ",",
    logical01 = T,
    bucket = bucket,
    object = object_name,
    col.names = F,
    row.names = F
  )
})

rm(store_dept_splits)
gc()

stopCluster(cl)
gc()

