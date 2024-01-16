import pandas as pd
def push_to_redshift(conn:str, df:pd.core.frame.DataFrame, table_name:str, insertion_type:str, 
                        schema='public')->None:
    """
    This function writes a pandas dataframe to a redshift data warehouse
    
    Parameters:
        conn: Redshift connection string
        df: Pandas DataFrame
        table_name: Name of redshift table
        insertion_type: Insertion type (Use either 'replace' of 'append')
        schema: The data warehouse schema where the table is located
    
    Returns: None
    """

    df.to_sql(name=table_name, con=conn, if_exists=insertion_type, schema=schema, index=False)
    print(f"Finished loading {df.shape[0]} records into {schema}.{table_name} Redshift table...")