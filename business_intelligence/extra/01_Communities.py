import pandas as pd
import numpy as np
import streamlit as st
import pandas as pd
from utils.load_data import load_multiple_data
from operator import itemgetter
import altair as alt
from streamlit_agraph import agraph, Node, Edge, Config, ConfigBuilder

dicc = {
    "ALSO_BUY": "asin",
    "ALSO_VIEW": "asin",
    "HAS_BRAND": "brand",
    "HAS_CATEGORY": "category",
    "HAS_INTENTION": "intention",
    "HAS_REVIEW": "review",
    "HAS_SENTIMENT": "sentiment",
    "IS_SIMILAR_TO": "asin"
}

st.set_page_config(page_title="Products Search", page_icon="")
data = pd.read_parquet("graph.parquet")
data.columns = ["src", "dst", "label", "weight"]
data = data[data.label != 'HAS_CATEGORY']
data = data[data.label != 'HAS_REVIEW']
products_graph = data
print(data.head())


st.title("Communities")
#st.dataframe(data)

category = st.selectbox("Filter by Category", options=[])
product = st.selectbox("Filter by Product", options=[
    "B00JM3R6M6", "B00J1ZSOTE", "B00KTPTRIW"
])

print(data["label"].unique())

st.markdown(product)

# Compute neighborhood

st.dataframe(data.groupby("label").count())
communities__graph = data[data.src == product]

#print(communities__graph.)
#filter_data.head()

g1 = products_graph[products_graph['src'] == 'B00005YDIC']

# Step 2: Join g1 and g2
result = pd.merge(g1[['dst']], products_graph[['src', 'dst', 'label', 'weight']], left_on='dst', right_on='dst', suffixes=('', '_g2'))

# Step 3: Join g2 and g3
#result = pd.merge(result[['src', 'dst', 'label', 'weight']], products_graph[['src', 'dst', 'label', 'weight']], left_on='src', right_on='src', suffixes=('_g2', ''))

# Select the desired columns from the final result
result = result[['src', 'dst', 'label', 'weight']]

print(result.head())

reviews = result[result.label == 'HAS_REVIEW']
not_reviews = result[result.label != 'HAS_REVIEW']
#reviews = reviews.sample(frac = 0.2)
#result = reviews.merge(not_reviews)
print(len(result))
result = not_reviews
print(len(result))


# Display the final result
communities__graph = result

#communities__graph = communities__graph[communities__graph.community_id == community_number]
src = communities__graph["src"].to_frame()

src["label"] = "asin"
dst = communities__graph["dst"].to_frame()
dst["label"] = communities__graph["label"].apply(lambda z: dicc[z])
dst.columns = ["src", "label"]
nodes = pd.concat([src, dst]).reset_index(drop = True)
nodes = nodes.drop_duplicates(subset=["src"])
data = data.to_dict()

ASIN_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/product.png"
BRAND_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/brand.png"
SENTIMENT_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/sentiment.png"
INTENTION_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/intention.png"
REVIEW_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/person.png"

node_image = {
    "asin": ASIN_IMAGE,
    "brand": BRAND_IMAGE,
    "sentiment": SENTIMENT_IMAGE,
    "intention": INTENTION_IMAGE, 
    "review": REVIEW_IMAGE
}
size_image = {
    "asin": 25,
    "brand": 50,
    "sentiment": 50,
    "intention": 50, 
    "review": 20
}

#print(data)
nodes = [Node(id = row.src, title = row.src, image = node_image[row.label], shape = "circularImage",  size = size_image[row.label]) for row in nodes.itertuples()] 
edges = [Edge(source = row.src, label = row.label, target = row.dst, width = row.weight, font = {"size": 0}) for row in communities__graph.itertuples()]

#print(nodes)
#print(edges)


#communities_data = load_data("share__chainbreaker_bi", "platinum", "communities_graph_obfuscated")

#communities_data = pd.read_parquet("./data/communities_graph.parquet")
#print(communities_data.head())
#st.dataframe(communities_data)

config = Config(
                width = 1024,
                height = 750,
                directed=True, 
                physics=True, 
                hierarchical=False,
                link = {"labelProperty": "HAS_PHONE", "renderLabel": True},
                highlightColor = "#58FF33",
                #collapsible=True,
                node = {"labelProperty": "label"}, 
                #maxZoom=2, 
                minZoom=0.1,
                #initialZoom=1, 
                solver = "forceAtlas2Based",
                #maxVelocity = 1,
                #minVelocity = 0.1,
                # **kwargs
                )
#config = config.build()

#config = Config(width=750,
#                height=950,
#                directed=True, 
#                physics=True, 
#                hierarchical=False,
#                link = {"labelProperty": "HAS_PHONE", "renderLabel": True},
#                # **kwargs
#                )
return_value = agraph(nodes=nodes, 
                      edges=edges, 
                      config=config)
