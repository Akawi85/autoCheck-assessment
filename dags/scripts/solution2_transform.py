from pandas import json_normalize
import json
import ast

def transform(response:str)->json:
    """
    This function transforms the response from https://xecdapi.xe.com/ API and returns the transformed data
    as a json object
    
    Parameters:
        response: Object returned from calling the xecd convert_from API
    
    Returns: DataFrame Object returned as json
    """
    norm_json = json.loads(json.dumps(ast.literal_eval(response)))

    # Flatten the nested 'from' list
    df = json_normalize(norm_json, record_path='to', meta=['from', 'amount', 'timestamp'])
    # Rename columns for clarity
    df = df.rename(columns={'from': 'currency_from', 'inverse': 'USD_to_currency_rate', 
                            'mid':'currency_to_USD_rate', 'quotecurrency': 'currency_to'})
    # select relevant columns
    df = df[['timestamp', 'currency_from', 'USD_to_currency_rate', 'currency_to_USD_rate', 'currency_to']]
    return df.to_json()