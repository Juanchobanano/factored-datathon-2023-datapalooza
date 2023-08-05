import pandas as pd
import numpy as np

data = pd.read_parquet("./data/degree_centrality.parquet")
print(data.head())

data["len"] = data.node.apply(lambda z: len(z))
data = data.sort_values(by = "degree_centrality_normalized", ascending=False)

results_asin = ['B0094X227I', 'B003ZSP0WW', 'B00004YRQ9', 'B003DSAT0C', 'B00004TN9O']
product_metadata = pd.read_parquet("./data/amazon_metadata.parquet")


results_asin_mask = product_metadata["asin"].isin(results_asin)
results_metadata = product_metadata[results_asin_mask].reset_index(drop=True).fillna("Not available")

print(results_metadata.head())
#print(data.groupby("len").count())


#product = "0078764343"
