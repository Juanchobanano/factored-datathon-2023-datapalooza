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
st.set_page_config(page_title="Real Time Analytics", page_icon="📈", layout="wide")
st.sidebar.title("About")
st.sidebar.info(
    """
    - [GitHub repository][1]

    [1]: https://github.com/Juanchobanano/factored-datathon-2023-datapalooza 
    """
)

st.sidebar.title("Contact")
st.sidebar.info(
    """
    [Juan Esteban Cepeda][1]\n
    [Cristhian Jose Pardo Mercado][2]\n
    [David Felipe Mora][3]\n
    [Eduards Alexis Chipatecua][4]
    
    [1]: https://www.linkedin.com/in/juan-e-cepeda-gestion/
    [2]: https://www.linkedin.com/in/cristhian-pardo/ 
    [3]: https://www.linkedin.com/in/davidfmora/
    [4]: https://www.linkedin.com/in/eduards-alexis-mendez-chipatecua-8584b21b4/
    """
)
# load pinecone index
pinecone.init(api_key=ct.PINECONE_API_KEY, environment=ct.PINECONE_ENVIRONMENT)
index = pinecone.Index(ct.PINECONE_INDEX_NAME)

data_list = [
    {"share": "share__hackaton__streamlit", "schema": "platinum", "table": "streaming_reviews_kpi_1"},
    {"share": "share__hackaton__streamlit", "schema": "platinum", "table": "streaming_reviews_kpi_2"},
    {"share": "share__hackaton__streamlit", "schema": "silver", "table": "amazon_metadata_silver_selected"},
    {"share": "share__hackaton__streamlit", "schema": "platinum", "table": "amazon_reviews_selected_with_intention_and_sentiment"},

]
data = load_multiple_data(data_list)
(
    streaming_reviews_kpi_1,
    streaming_reviews_kpi_2,
    products_metadata,
    amazon_reviews_selected_with_intention_and_sentiment
 
) = itemgetter(
    "streaming_reviews_kpi_1",
    "streaming_reviews_kpi_2",
    "amazon_metadata_silver_selected",
    "amazon_reviews_selected_with_intention_and_sentiment"
 
)(data)

# main title
st.title("📈 Real Time Analytics")

col1, col2 = st.columns(2)
with col1:
    st.subheader("💪 Factored EventHub")
    st.divider()
    st.metric(
    label="Total Factored EventHub Unique Streaming Data",
    value=59954,
    delta= f"{round(100*(59954-48306)/59954,2)}%" #TODO: sacarlos los totales de los kpis
    )
    # st.divider()
    st.line_chart(streaming_reviews_kpi_1, x="day")
with col2:
    st.subheader("🚀 Datalaooza EventHub")
    st.divider()
    # st.dataframe(streaming_reviews_kpi_2)
    st.metric(
    label="Total Datapalooza EventHub Streaming Data",
    value=6141,
    delta= f"{round(100*(6141-6098)/6141,2)}%" #TODO: sacarlos los totales de los kpis
    )
    # st.divider()
    st.line_chart(streaming_reviews_kpi_2, x="date")


st.divider()

st.markdown("Utilize semantic search to find products effortlessly. Explore Intentions and Semtiment on streaming captured data!🚀")
st.caption("Start by trying something like 'Nintendo Entertainment System'")
query_text = st.text_input("🛍️ Search a product")
max_results = st.slider(
    "🎛️ Select the maximum number of elements you want to retrieve", 
    min_value = 5, 
    max_value = 30, 
    step = 5, 
    key = "slider_1"
)


# If query text not empty
if query_text:

    # Semantic search
    xq = emb_text(query_text)
    xc = index.query(xq, top_k=max_results)         # Get top max_results relevant products
    results_asin = [x["id"] for x in xc["matches"]] # Get ids of asins

    # Filter results metadata and reviews
    results_asin_mask = products_metadata["asin"].isin(results_asin)
    results_metadata = products_metadata[results_asin_mask].reset_index(drop=True).fillna("Not available")

    # Display query results
    options = list(results_metadata["title"].values)
    product_idx = st.selectbox("📊 Query results", range(len(options)), format_func=lambda x: options[x])
    asin = results_asin[product_idx]

    filtered_df = amazon_reviews_selected_with_intention_and_sentiment[amazon_reviews_selected_with_intention_and_sentiment.asin == asin]
    bool_ = (len(filtered_df) == 0)
    print(bool_)

    #check if product is in streaming data
    st.markdown(f"The product {options[product_idx]} has reviews from streaming data?")
    if bool_:
        st.markdown("NO")
    else:
        st.markdown("Yes")
        

    if not bool_:
        st.dataframe(filtered_df)       
