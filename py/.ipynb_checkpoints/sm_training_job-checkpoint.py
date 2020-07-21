# Libraries
import boto3
from sagemaker import get_execution_role
import sagemaker.amazon.common as smac
import argparse
import time

# Execution role
role = get_execution_role()

# S3 bucket and prefix
bucket = 'abn-distro'
prefix = 'm5_store_items'

# Input args
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--id", required=True, help="nodel id")
ap.add_argument("-d", "--data", required=True, help="training data final prefix")
ap.add_argument("-t", "--dist", required=True, help="s3 distribution type")
ap.add_argument("-c", "--cnt", type=int, required=True, help="instance count")
ap.add_argument("-r", "--maxrun", type=int, required=True, help="max runtime")
args = vars(ap.parse_args())

# XGBoost container
from sagemaker.amazon.amazon_estimator import get_image_uri
container = get_image_uri(boto3.Session().region_name, 'xgboost')

# Training parameters
sharded_training_params = {
    "RoleArn": role,
    "AlgorithmSpecification": {
        "TrainingImage": container,
        "TrainingInputMode": "File"
    },
    "ResourceConfig": {
        "InstanceCount": args["cnt"],
        "InstanceType": "ml.m5.4xlarge",
        "VolumeSizeInGB": 20
    },
    "InputDataConfig": [
        {
            "ChannelName": "train",
            "ContentType": "csv",
            "DataSource": {
                "S3DataSource": {
                    "S3DataDistributionType": args["dist"],
                    "S3DataType": "S3Prefix",
                    "S3Uri": "s3://{}/{}/{}/".format(bucket, prefix, args["data"])
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
        "min_child_weight": "7",
        "colsample_bytree": "0.8",
        "subsample": "0.8",
        "max_depth": "10"
    },
    "StoppingCondition": {
        "MaxRuntimeInSeconds": args["maxrun"]
    }
}

# Training job name
sharded_job = args["id"] + "-" + args["dist"] + "-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
sharded_training_params['TrainingJobName'] = sharded_job

# Launch training job 
region = boto3.Session().region_name
sm = boto3.Session().client('sagemaker')
sm.create_training_job(**sharded_training_params)

# Describe status
status = sm.describe_training_job(TrainingJobName=sharded_job)['TrainingJobStatus']
print("Training job ended with status: " + status)
if status == 'Failed':
    message = sm.describe_training_job(TrainingJobName=sharded_job)['FailureReason']
    print('Training failed with the following error: {}'.format(message))
    raise Exception('Training job failed')
