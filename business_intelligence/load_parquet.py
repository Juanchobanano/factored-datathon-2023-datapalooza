import pandas as pd
import numpy as np

data = pd.read_parquet("graph.parquet")
print(data.head())

print(data.groupby("label").count())
print(len(data))
data = data[data.label != 'HAS_REVIEW']
print(len(data))
#product = "0078764343"
