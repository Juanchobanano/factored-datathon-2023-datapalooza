import pandas as pd
import numpy as np
import streamlit as st
import pandas as pd
from utils.load_data import load_multiple_data
from utils.embeddings import emb_text
from operator import itemgetter
import altair as alt
import pinecone
from streamlit_agraph import agraph, Node, Edge, Config, ConfigBuilder
import constants as ct


# page title
st.set_page_config(page_title="Product Search", page_icon="🔍")
# load pinecone index
pinecone.init(api_key=ct.PINECONE_API_KEY, environment=ct.PINECONE_ENVIRONMENT)
index = pinecone.Index(ct.PINECONE_INDEX_NAME)

# load data
data_list = [
    {"share": "share__hackaton__streamlit", "schema": "gold", "table": "products_graph"},
    {"share": "share__hackaton__streamlit", "schema": "silver", "table": "amazon_reviews_selected"},
    {"share": "share__hackaton__streamlit", "schema": "silver", "table": "amazon_metadata_silver_selected"},
    {"share": "share__hackaton__streamlit", "schema": "gold", "table": "degree_centrality_normalized"},
    {"share": "share__hackaton__streamlit", "schema": "gold", "table": "betweenes_centrality"},
]
data = load_multiple_data(data_list)
(
    products_graph,
    product_reviews,
    product_metadata,
    degree_centrality,
    betweenness_centrality,
) = itemgetter(
    "products_graph",
    "amazon_reviews_selected",
    "amazon_metadata_silver_selected",
    "degree_centrality_normalized",
    "betweenes_centrality",
)(data)

# main title
st.title("🔍 Product Search")

# search bar
query_text = st.text_input("Search products by title", value="Nintendo 64")
# embed text and query pinecone for relevant products
xq = emb_text(query_text)
xc = index.query(xq, top_k=5)  # top 5 relevant products
results_asin = [x["id"] for x in xc["matches"]]
# filter results metadata and reviews
results_asin_mask = product_metadata["asin"].isin(results_asin)
results_metadata = product_metadata[results_asin_mask].reset_index(drop=True).fillna("Not Available")
results_reviews = product_reviews[product_reviews["asin"].isin(results_asin)].reset_index(drop=True)

## display dataframes for debugging
# st.dataframe(results_metadata)
# st.dataframe(results_reviews)

# Product Information
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.subheader("🏷️ Title")
    title = results_metadata["title"].tolist()
    st.markdown("\n\n".join(title))
with col2:
    st.subheader("🏢 Brand")
    brand = results_metadata["brand"].tolist()
    st.markdown("\n\n".join(brand))
with col3:
    st.subheader("💵 Price")
    price = results_metadata["mean_price"].astype(str).tolist()
    st.markdown("\n\n".join(price))
with col4:
    st.subheader("🎯 Graph Analysis")
    button_1 = st.button("Analize product", key=1)
    button_2 = st.button("Analize product", key=2)
    button_3 = st.button("Analize product", key=3)
    button_4 = st.button("Analize product", key=4)
    button_5 = st.button("Analize product", key=5)

product_idx = 0
if button_1:
    product_idx = 0
elif button_2:
    product_idx = 1
elif button_3:
    product_idx = 2
elif button_4:
    product_idx = 3
elif button_5:
    product_idx = 4

# selected product metadata and reviews
product_metadata = results_metadata.iloc[product_idx]
product_reviews = results_reviews.iloc[product_idx]
product_asin = product_metadata["asin"]

st.subheader(product_metadata["title"])


# show additional product information
st.subheader("Additional Product Information")
additional_info = product_metadata[["description", "feature", "category", "mean_price"]]
st.dataframe(additional_info)

# show product reviews
st.subheader("📝 Product Reviews")
show_amazon_reviews = product_reviews[
    ["reviewID", "date", "reviewerName", "summary", "reviewText", "verified", "vote"]
]
st.dataframe(show_amazon_reviews)

# Filter the dataframe using masks
# m1 = df["Autor"].str.contains(text_search)
# m2 = df["Título"].str.contains(text_search)
# df_search = df[m1 | m2]

# Show product subgraph
st.subheader("🏘️ Product Neighborhood")

### UNCOMEENT THIS TO SHOW THE GRAPH
# # Show degree and betweenness centrality metrics
# col1, col2 = st.columns(2)
# with col1:
#     value = degree_centrality[degree_centrality.node == product_asin]["degree_centrality"].values[0]
#     value = round(value, 3)
#     st.metric(label="Product Local Degree Centrality (from 0 to 1)", value=value)
#     st.caption(
#         "Measure how popular a product is, helping companies to allocate marketing budgets more effectively and focus on products with high customer engagement."
#     )
# with col2:
#     value = betweenness_centrality[betweenness_centrality.node == product_asin][
#         "betweenes_centrality_normalized"
#     ].values[0]
#     value = round(value, 3)
#     st.metric(label="Product Local Betweenness Centrality (from 0 to 1)", value=value)
#     st.caption(
#         "Measures how influential a product is, helping companies target their marketing efforts more effectively. Collaborating with influential brands or featuring influential products can boost exposure and sales."
#     )

# # Get graph
# g1 = products_graph[products_graph["src"] == product_asin]
# result = pd.merge(
#     g1[["dst"]], products_graph[["src", "dst", "label", "weight"]], left_on="dst", right_on="dst", suffixes=("", "_g2")
# )
# result = result[["src", "dst", "label", "weight"]]
# main = result[(result.src == product_asin) | (result.label == "HAS_REVIEW")]  # & (result.src == product))]
# not_main = result[(result.src != product_asin) & (result.label != "HAS_REVIEW")]
# not_main = not_main.sample(n=200)
# result = pd.concat([main, not_main])
# communities__graph = result
# src = communities__graph["src"].to_frame()
# src["label"] = "asin"
# dst = communities__graph["dst"].to_frame()
# dst["label"] = communities__graph["label"].apply(lambda z: ct.dicc[z])
# dst.columns = ["src", "label"]
# nodes = pd.concat([src, dst]).reset_index(drop=True)
# nodes = nodes.drop_duplicates(subset=["src"])

# # Prepare graph.
# nodes_results = list()
# for row in nodes.itertuples():
#     if row.src == product_asin:
#         node = Node(
#             id=row.src,
#             title=row.src,
#             image=ct.node_image[row.label],
#             shape="circularImage",
#             size=50,
#             border=50,
#             color="yellow",
#             shadow=True,
#             label=row.src,
#             font={},
#         )
#     else:
#         node = Node(
#             id=row.src,
#             title=row.src,
#             image=ct.node_image[row.label],
#             shape="circularImage",
#             size=ct.size_image[row.label],
#         )
#     nodes_results.append(node)

# edges = [
#     Edge(source=row.src, label=row.label, target=row.dst, font={"size": ct.edge_font_size[row.label]})
#     for row in communities__graph.itertuples()
# ]

# config = Config(
#     width=750,  # 1024,
#     height=750,  # 750,
#     directed=True,
#     physics=True,
#     hierarchical=False,
#     link={"labelProperty": "HAS_PHONE", "renderLabel": True},
#     highlightColor="#58FF33",
#     # collapsible=True,
#     node={"labelProperty": "label"},
#     # maxZoom=2,
#     minZoom=0.1,
#     # initialZoom=1,
#     solver="forceAtlas2Based",
# )

# # Show graph.
# return_value = agraph(nodes=nodes_results, edges=edges, config=config)
