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
degree_centrality = pd.read_parquet("./data/degree_centrality.parquet")
betweennes_centrality = pd.read_parquet("./data/betweenness_centrality.parquet")

asins = list(amazon_metadata["asin"].unique())
degree_centrality.sort_values("degree_centrality_normalized", ascending = False)
degree_centrality["inlist"] = degree_centrality.node.apply(lambda z: z in asins)
degree_centrality = degree_centrality[degree_centrality.inlist == True]

st.dataframe(degree_centrality.sort_values("degree_centrality_normalized", ascending = False))