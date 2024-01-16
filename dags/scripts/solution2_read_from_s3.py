import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.client import Config

def read_from_s3(bucket_name:str, key:str)->pd.core.frame.DataFrame:
    """
    This function reads a CSV file from specified s3 bucket and returns it as a Pandas DataFrame
    
    Parameters:
        bucket_name: Name of s3 bucket to read from
        key: AWS s3 object name (file path to the CSV in s3)
    
    Returns: DataFrame from s3
    """
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    read_file = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(read_file['Body'], on_bad_lines='warn')
    print("Successfully read data from s3")
    return df