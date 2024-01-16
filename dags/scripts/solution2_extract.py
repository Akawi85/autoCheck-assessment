import requests
import json

def extract(base_url:str, auth:requests.auth.HTTPBasicAuth, params:dict)->json:
    """
    This function gets daily updated exchange rates from xecdapi API and returns an API response object

    Parameters:
        base_url: The base URL string for xecd API
        auth: The API authentication object which takes the 'Account API ID' and 'Account API Key'
        params: A dictionary of acceptable API parameters.

    Returns: API response object
    """
    response = requests.get(base_url+'/v1/convert_from', auth=auth, params=params)

    return response.json()