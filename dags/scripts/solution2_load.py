from io import StringIO
import json, ast
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from solution2_read_from_s3 import read_from_s3

def load_to_s3(df_new:pd.core.frame.DataFrame, bucket_name:str, key:str)->None:
    """
    This function writes the newly read dataframe from the transform layer,
    concatenates the values with the data already existing in the destination s3 bucket, 
    checks for and drops duplicates and finally writes the new non-duplicated dataframe to the 
    specified s3 bucket in csv format
    
    Parameters:
        df_new: The new Pandas DataFrame from the transform layer you intend to write to s3
        bucket_name: Name of s3 bucket to write to
        key: AWS s3 object name (file path to save DataFrame in s3)
    
    Returns: None
    """
    norm_json = json.loads(json.dumps(ast.literal_eval(df_new)))
    df_new = pd.DataFrame(norm_json)
    df_old = read_from_s3(bucket_name=bucket_name, key=key)
    df_main = pd.concat([df_old, df_new])
    df_main.drop_duplicates(inplace=True)
    csv_buffer = StringIO()
    df_main.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    s3_resource.Object(bucket_name, key).put(Body=csv_buffer.getvalue())
    print("Successfully wrote data to s3")