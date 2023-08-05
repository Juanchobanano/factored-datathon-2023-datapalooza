import streamlit as st
import pandas as pd
from utils.load_data import load_multiple_data
from operator import itemgetter
import altair as alt

st.set_page_config(page_title="Timeseries", page_icon="")
data_list = [
    {"share": "share__products_bi", "schema": "platinum", "table": "reviews_count_per_day"}
]

data = load_multiple_data(data_list)
(
    reviews_count_per_day
) = itemgetter("reviews_count_per_day")(data)

st.title("Time series")
st.dataframe(reviews_count_per_day)