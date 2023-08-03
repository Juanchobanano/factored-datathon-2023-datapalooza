import streamlit as st
import pandas as pd
import utils
from utils.load_data import load_multiple_data
from operator import itemgetter
import altair as alt


page_title="General Metrics"
st.set_page_config(page_title, page_icon="")
data_list = [
    {"share": "share__products_bi", "schema": "platinum", "table": "avg_words_per_review"},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'count_reviews_per_category'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'count_reviews_per_category_and_overall'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'count_reviews_per_overall'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'count_reviews_per_product_per_category'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'count_similar_products_between_categories'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'count_verified_reviews_per_category'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'distribution_of_description'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'distrobution_images'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'mean_ratings_per_asin'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'overall_distribution_per_product'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'price_comparison_between_categories'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'similar_items_per_asin'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'total_products_per_brand'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'total_products_per_category'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'total_reviews_per_reviewerid'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'verified_distribution'},
    {'share': 'share__products_bi', 'schema': 'platinum', 'table': 'vote_distribution'}
]

data = load_multiple_data(data_list)

(
    avg_words_per_review,
    count_reviews_per_category,
    count_reviews_per_category_and_overall,
    count_reviews_per_overall,
    count_reviews_per_product_per_category,
    count_similar_products_between_categories,
    count_verified_reviews_per_category,
    distribution_of_description,
    distrobution_images,
    mean_ratings_per_asin,
    overall_distribution_per_product,
    price_comparison_between_categories,
    similar_items_per_asin,
    total_products_per_brand,
    total_products_per_category,
    total_reviews_per_reviewerid,
    verified_distribution,
    vote_distribution
) =  itemgetter(
    "avg_words_per_review",
    "count_reviews_per_category",
    "count_reviews_per_category_and_overall",
    "count_reviews_per_overall",
    "count_reviews_per_product_per_category",
    "count_similar_products_between_categories",
    "count_verified_reviews_per_category",
    "distribution_of_description",
    "distrobution_images",
    "mean_ratings_per_asin",
    "overall_distribution_per_product",
    "price_comparison_between_categories",
    "similar_items_per_asin",
    "total_products_per_brand",
    "total_products_per_category",
    "total_reviews_per_reviewerid",
    "verified_distribution",
    "vote_distribution "
    )(data)

st.title(page_title)

st.dataframe(avg_words_per_review)
st.dataframe(count_reviews_per_category)
st.dataframe(count_reviews_per_category_and_overall)
st.dataframe(count_reviews_per_overall)
st.dataframe(count_reviews_per_product_per_category)
st.dataframe(count_similar_products_between_categories)
st.dataframe(count_verified_reviews_per_category)
st.dataframe(distribution_of_description)
st.dataframe(distrobution_images)
st.dataframe(mean_ratings_per_asin)
st.dataframe(overall_distribution_per_product)
st.dataframe(price_comparison_between_categories)
st.dataframe(similar_items_per_asin)
st.dataframe(total_products_per_brand)
st.dataframe(total_products_per_category)
st.dataframe(total_reviews_per_reviewerid)
st.dataframe(verified_distribution)
st.dataframe(vote_distribution)