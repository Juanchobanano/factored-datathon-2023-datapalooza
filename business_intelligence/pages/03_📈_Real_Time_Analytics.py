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
st.set_page_config(page_title="Real Time Analytics", page_icon="📈")
# load pinecone index
pinecone.init(api_key=ct.PINECONE_API_KEY, environment=ct.PINECONE_ENVIRONMENT)
index = pinecone.Index(ct.PINECONE_INDEX_NAME)

data_list = [
    {"share": "share__hackaton__streamlit", "schema": "platinum", "table": "streaming_reviews_kpi_1"},
    {"share": "share__hackaton__streamlit", "schema": "platinum", "table": "streaming_reviews_kpi_2"},

]
data = load_multiple_data(data_list)
(
    streaming_reviews_kpi_1,
    streaming_reviews_kpi_2
) = itemgetter(
    "streaming_reviews_kpi_1",
    "streaming_reviews_kpi_2"
)(data)

# main title
st.title("📈 Real Time Analytics")

col1, col2 = st.columns(2)
with col1:
    st.subheader("💪 Factored EventHub")
    st.metric(
    label="Total Factored EventHub Streaming Data",
    value=10,
    delta= 5)
    st.line_chart(streaming_reviews_kpi_1)
with col2:
    st.subheader("🚀 Datalaooza EventHub")
    st.metric(
    label="Total Datapalooza EventHub Streaming Data",
    value=10,
    delta= 5)
    st.line_chart(streaming_reviews_kpi_2)


st.divider()

st.markdown("Utilize semantic search to find products effortlessly. Explore Intentions and Semtiment on streaming captured data!🚀")
st.caption("Start by trying something like 'Nintendo 64'")
query_text = st.text_input("🛍️ Search a product")



# If query text not empty
if query_text:

    # Semantic search
    xq = emb_text(query_text)
    xc = index.query(xq, top_k=max_results)         # Get top max_results relevant products
    results_asin = [x["id"] for x in xc["matches"]] # Get ids of asins

    # Filter results metadata and reviews
    results_asin_mask = product_metadata["asin"].isin(results_asin)
    results_metadata = product_metadata[results_asin_mask].reset_index(drop=True).fillna("Not available")

    # Display query results
    options = list(results_metadata["title"].values)
    product_idx = st.selectbox("📊 Query results", range(len(options)), format_func=lambda x: options[x])
  
