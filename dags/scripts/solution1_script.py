import os
import glob
import pandas as pd

def main(engine, data_path, script_path):
    """
    This function uploads the sample assessment to a postgres database, runs the SQL query against the database
    and get the transformed data output, which is saved as excel file
    """
    all_files = glob.glob(os.path.join(data_path , "*.csv"))

    for full_file_path in all_files:
        file_names = os.path.splitext(os.path.basename(full_file_path))[0]
        df = pd.read_csv(full_file_path)
        df.to_sql(name=file_names, schema='public', con=engine, if_exists='replace', index=False)
    print("Successfully wrote data to postgres database")
    
    for file in os.listdir(script_path):
        if file.endswith(".sql"):
            full_file_path = f"{script_path}/{file}"
            with open(full_file_path, 'r') as file_handle:
                df_transformed=pd.read_sql(file_handle.read(), con=engine)
                df_transformed.to_excel(data_path+'output_name.xlsx', sheet_name='loans_data', index=False)
                print("Successfully queried the databse and returned the result as a dataframe")