import streamlit as st 
import delta_sharing
import constants
import pandas as pd 
import numpy as np
import concurrent.futures

def load_data(share, schema, table):
    url = constants.profile_file + f"#{share}.{schema}.{table}"
    pandas_df = delta_sharing.load_as_pandas(url)
    return pandas_df

@st.cache_data(ttl=0, show_spinner="Fetching data from API...")
def load_multiple_data(data_list):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = {}
        futures = {executor.submit(load_data, data['share'], data['schema'], data['table']): data['table'] for data in data_list}
        
        for future in concurrent.futures.as_completed(futures):
            table = futures[future]
            try:
                result = future.result()
                results[table] = result
            except Exception as e:
                results[table] = e  # Si ocurre algún error, se guarda en el diccionario de resultados
    return results