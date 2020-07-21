import boto3
from sagemaker import get_execution_role
import sagemaker.amazon.common as smac
import argparse

# Execution role
role = get_execution_role()

# Input args
ap = argparse.ArgumentParser()
ap.add_argument("-m", "--modelname", required=True, help="model name")
ap.add_argument("-u", "--modelurl", required=True, help="model data url")
args = vars(ap.parse_args())

# Configure SageMaker
region = boto3.Session().region_name
sm = boto3.Session().client('sagemaker')

# Xgboost container
from sagemaker.amazon.amazon_estimator import get_image_uri
container = get_image_uri(boto3.Session().region_name, 'xgboost')

# Create model
sharded_model_response = sm.create_model(
    ModelName=args["modelname"],
    ExecutionRoleArn=role,
    PrimaryContainer={
        'Image': container,
        'ModelDataUrl': args["modelurl"]
    }
)