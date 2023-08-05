import streamlit as st
import delta_sharing
import json
from os.path import exists

PINECONE_API_KEY = st.secrets["PINECONE_API_KEY"]
PINECONE_ENVIRONMENT = st.secrets["PINECONE_ENVIRONMENT"]
PINECONE_INDEX_NAME = st.secrets["PINECONE_INDEX_NAME"]
HF_API_KEY = st.secrets["HF_API_KEY"]

config_share = {
    "shareCredentialsVersion": st.secrets.delta_sharing_credentials.shareCredentialsVersion,
    "bearerToken": st.secrets.delta_sharing_credentials.bearerToken,
    "endpoint": st.secrets.delta_sharing_credentials.endpoint,
    "expirationTime": st.secrets.delta_sharing_credentials.expirationTime,
}
profile_file = "./config.share"
if not exists(profile_file):
    with open(profile_file, "w") as f:
        f.write(json.dumps(config_share))

client = delta_sharing.SharingClient(profile_file)

dicc = {
    "ALSO_BUY": "asin",
    "ALSO_VIEW": "asin",
    "HAS_BRAND": "brand",
    "HAS_CATEGORY": "category",
    "HAS_INTENTION": "intention",
    "HAS_REVIEW": "review",
    "HAS_SENTIMENT": "sentiment",
    "IS_SIMILAR_TO": "asin",
}

ASIN_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/product.png"
BRAND_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/brand.png"
SENTIMENT_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/sentiment.png"
INTENTION_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/intention.png"
REVIEW_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/person.png"
CATEGORY_IMAGE = "https://datapalooza-public-streamlit-images.s3.amazonaws.com/category.png"

node_image = {
    "asin": ASIN_IMAGE,
    "brand": BRAND_IMAGE,
    "sentiment": SENTIMENT_IMAGE,
    "intention": INTENTION_IMAGE,
    "review": REVIEW_IMAGE,
    "category": CATEGORY_IMAGE,
}

size_image = {"asin": 25, "brand": 50, "sentiment": 50, "intention": 50, "review": 40, "category": 50}

edge_font_size = {
    "ALSO_BUY": 20,
    "ALSO_VIEW": 20,
    "HAS_BRAND": 0,
    "HAS_CATEGORY": 0,
    "HAS_INTENTION": 0,
    "HAS_REVIEW": 20,
    "HAS_SENTIMENT": 0,
    "IS_SIMILAR_TO": 20,
}
