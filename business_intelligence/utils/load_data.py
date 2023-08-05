import streamlit as st
from tqdm import tqdm
import delta_sharing
import constants
import concurrent.futures


def load_data(share, schema, table):
    url = constants.profile_file + f"#{share}.{schema}.{table}"
    pandas_df = delta_sharing.load_as_pandas(url)
    return pandas_df


@st.cache_data(show_spinner="Fetching data from API...")
def load_multiple_data(data_list):
    pbar = tqdm(desc="Loading data", total=len(data_list))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = {}
        futures = {
            executor.submit(load_data, data["share"], data["schema"], data["table"]): data["table"]
            for data in data_list
        }

        for future in concurrent.futures.as_completed(futures):
            table = futures[future]
            try:
                result = future.result()
                results[table] = result
            except Exception as e:
                results[table] = e  # Si ocurre algún error, se guarda en el diccionario de resultados
            pbar.update(1)
    pbar.close()
    return results
