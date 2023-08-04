import pandas as pd
import numpy as np
import streamlit as st
import pandas as pd
from utils.load_data import load_multiple_data
from operator import itemgetter
import altair as alt
from streamlit_agraph import agraph, Node, Edge, Config, ConfigBuilder
import constants as ct

# Page title
st.set_page_config(page_title="Products Search", page_icon="")

# Load parquets
products_graph = pd.read_parquet("./data/graph.parquet")
amazon_reviews = pd.read_parquet("./data/amazon_reviews.parquet")
amazon_metadata = pd.read_parquet("./data/amazon_metadata.parquet")

# Main title
st.title("🔍 Products Search")

product_title = st.text_input("Search products by title", value = "Nintendo 64")

# Searh bar
product = "B00005YDIC"
amazon_metadata = amazon_metadata[amazon_metadata.asin == product]
amazon_reviews = amazon_reviews[amazon_reviews.asin == product]

# Product Information
col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("🏷️ Title")
    st.markdown(f"#### {product_title}")
with col2: 
    st.subheader("🏢 Brand")
    brand = amazon_metadata["brand"].values[0]
    st.markdown(f"#### {brand}")
with col3: 
    st.subheader("📂 Category")
    category = amazon_metadata["main_category"].values[0]
    st.markdown(f"#### {category}")

# Show additional product information
st.subheader("ℹ️ Additional Product Information")
additional_info = amazon_metadata[["description", "feature", "category", "mean_price"]]
st.dataframe(additional_info)

# Show product reviews
st.subheader("📝 Product Reviews")
show_amazon_reviews = amazon_reviews[["reviewID", "date", "reviewerName", "summary", "reviewText", "verified", "vote"]]
st.dataframe(show_amazon_reviews)
# Filter the dataframe using masks
#m1 = df["Autor"].str.contains(text_search)
#m2 = df["Título"].str.contains(text_search)
#df_search = df[m1 | m2]

# Show product subgraph
st.subheader("🏘️ Product Neighborhood")

# Show degree and betweenness centrality metrics
col1, col2 = st.columns(2)
with col1: 
    st.metric(label = "Product Local Degree Centrality (from 0 to 1)", value = 0.7)
    st.caption("Measure how popular a product is, helping companies to allocate marketing budgets more effectively and focus on products with high customer engagement.")
with col2: 
    st.metric(label = "Product Local Betweenness Centrality (from 0 to 1)", value = 0.6)
    st.caption("Measures how influential a product is, helping companies target their marketing efforts more effectively. Collaborating with influential brands or featuring influential products can boost exposure and sales.")

# Get graph
g1 = products_graph[products_graph['src'] == product]
result = pd.merge(g1[['dst']], products_graph[['src', 'dst', 'label', 'weight']], left_on='dst', right_on='dst', suffixes=('', '_g2'))
result = result[['src', 'dst', 'label', 'weight']]
main = result[(result.src == product) | (result.label == 'HAS_REVIEW')]# & (result.src == product))]
not_main = result[(result.src != product) & (result.label != 'HAS_REVIEW')]
not_main = not_main.sample(n = 200)
result = pd.concat([main, not_main])
communities__graph = result
src = communities__graph["src"].to_frame()
src["label"] = "asin"
dst = communities__graph["dst"].to_frame()
dst["label"] = communities__graph["label"].apply(lambda z: ct.dicc[z])
dst.columns = ["src", "label"]
nodes = pd.concat([src, dst]).reset_index(drop = True)
nodes = nodes.drop_duplicates(subset=["src"])

# Prepare graph.
nodes_results = list()
for row in nodes.itertuples():
    if row.src == product:
        node = Node(
            id = row.src, 
            title = row.src, 
            image = ct.node_image[row.label], 
            shape = "circularImage", 
            size = 50, 
            border = 50, 
            color = "yellow", 
            shadow = True, 
            label = row.src, 
            font = {}
        )
    else:
        node = Node(id = row.src, title = row.src, image = ct.node_image[row.label], shape = "circularImage", size = ct.size_image[row.label])
    nodes_results.append(node)

edges = [Edge(source = row.src, label = row.label, target = row.dst, font = {"size": ct.edge_font_size[row.label]}) for row in communities__graph.itertuples()]

config = Config(
                width = 750, #1024,
                height = 750, #750,
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
                )

# Show graph.
return_value = agraph(nodes=nodes_results, 
                    edges=edges, 
                    config=config)