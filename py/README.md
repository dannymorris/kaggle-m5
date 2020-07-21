## Usage

### Train by store id

```
python3 sm_training_job.py -i "m5-store" -d "train_stores" -t "FullyReplicated" -c 10 -r 18000
python3 sm_training_job.py -i "m5-store" -d "train_stores" -t "ShardedByS3Key" -c 10 -r 18000
```

### Train by store-dept combinations

```
python3 sm_training_job.py -i "m5-store-dept" -d "train_store_dept" -t "FullyReplicated" -c 10 -r 18000
python3 sm_training_job.py -i "m5-store-dept" -d "train_store_dept" -t "ShardedByS3Key" -c 10 -r 18000
```