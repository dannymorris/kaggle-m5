##################
## Create model ##
##################

region = boto3.Session().region_name
sm = boto3.Session().client('sagemaker')

from sagemaker.amazon.amazon_estimator import get_image_uri
container = get_image_uri(boto3.Session().region_name, 'xgboost')

model_name = 'm5-sharded-xgboost-2020-07-19-17-02-04'
model_url = 's3://abn-distro/m5_store_items/m5-sharded-xgboost-2020-07-19-17-02-04/output/model.tar.gz'

sharded_model_response = sm.create_model(
    ModelName=model_name,
    ExecutionRoleArn=role,
    PrimaryContainer={
        'Image': container,
        'ModelDataUrl': model_url
    }
)