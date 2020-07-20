#############
# Libraries #
#############

import boto3
from sagemaker import get_execution_role
import sagemaker.amazon.common as smac
import time
import json

########################
# S3 bucket and prefix #
########################

bucket = 'abn-distro'
prefix = 'm5_store_items'

####################
## Execution role ##
####################

role = get_execution_role()

#######################
## XGBoost container ##
#######################

from sagemaker.amazon.amazon_estimator import get_image_uri
container = get_image_uri(boto3.Session().region_name, 'xgboost')

#########################
## Training parameters ##
#########################

sharded_training_params = {
    "RoleArn": role,
    "AlgorithmSpecification": {
        "TrainingImage": container,
        "TrainingInputMode": "File"
    },
    "ResourceConfig": {
        "InstanceCount": 10,
        "InstanceType": "ml.m4.4xlarge",
        "VolumeSizeInGB": 10
    },
    "InputDataConfig": [
        {
            "ChannelName": "train",
            "ContentType": "csv",
            "DataSource": {
                "S3DataSource": {
                    "S3DataDistributionType": "ShardedByS3Key",
                    "S3DataType": "S3Prefix",
                    "S3Uri": "s3://{}/{}/train_stores/".format(bucket, prefix)
                }
            },
            "CompressionType": "None",
            "RecordWrapperType": "None"
        },
    ],
    "OutputDataConfig": {
        "S3OutputPath": "s3://{}/{}/".format(bucket, prefix)
    },
    "HyperParameters": {
        "num_round": "1500",
        "eta": "0.1",
        "objective": "reg:tweedie",
        "tweedie_variance_power": "1.1",
        "eval_metric": "rmse",
        "rate_drop": "0.2",
        "min_child_weight": "7",
        "colsample_bytree": "0.8",
        "subsample": "0.8"
    },
    "StoppingCondition": {
        "MaxRuntimeInSeconds": 24000
    }
}

#######################
## Training job name ##
#######################

sharded_job = 'm5-sharded-xgboost-' + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
sharded_training_params['TrainingJobName'] = sharded_job

#########################
## Launch training job ##
#########################

region = boto3.Session().region_name
sm = boto3.Session().client('sagemaker')

sm.create_training_job(**sharded_training_params)

status = sm.describe_training_job(TrainingJobName=sharded_job)['TrainingJobStatus']
print(status)

sm.get_waiter('training_job_completed_or_stopped').wait(TrainingJobName=sharded_job)

status = sm.describe_training_job(TrainingJobName=sharded_job)['TrainingJobStatus']
print("Training job ended with status: " + status)

if status == 'Failed':
    message = sm.describe_training_job(TrainingJobName=sharded_job)['FailureReason']
    print('Training failed with the following error: {}'.format(message))
    raise Exception('Training job failed')