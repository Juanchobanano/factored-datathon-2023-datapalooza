import streamlit as st
import pandas as pd
import utils
from utils.load_data import load_multiple_data
from operator import itemgetter
import altair as alt

import plotly.express as px
import plotly.graph_objects as go


page_title="General Metrics"
st.set_page_config(
    page_title, 
    page_icon="",
    layout= "wide")

data_list = [
    {"share": "share__hackaton__streamlit", "schema": "platinum", "table": "avg_words_per_review"},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'count_reviews_per_category'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'count_reviews_per_category_and_overall'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'count_reviews_per_overall'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'count_reviews_per_product_per_category'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'count_similar_products_between_categories'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'count_verified_reviews_per_category'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'distribution_of_description'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'distrobution_images'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'mean_ratings_per_asin'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'overall_distribution_per_product'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'price_comparison_between_categories'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'similar_items_per_asin'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'total_products_per_brand'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'total_products_per_category'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'total_reviews_per_reviewerid'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'verified_distribution'},
    {'share': 'share__hackaton__streamlit', 'schema': 'platinum', 'table': 'vote_distribution'}
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
    "vote_distribution"
    )(data)



#Valores de la cabeza del proyecto
#total_products_per_category
#count_similar_products_between_categories
#count_verified_reviews_per_category
#count_reviews_per_category

st.title("Discover the Magical Data Visualization! An Experience That Will Leave You Speechless!")


    # create three columns
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

# fill in those three columns with respective metrics or KPIs
kpi1.metric(
    label="total products per category",
    value=f"{total_products_per_category['total_products'][0] , total_products_per_category['total_products'][1]}",
    delta= 5,
)

kpi2.metric(
    label="Married Count 💍",
    value=12,
    delta=66,
)

kpi3.metric(
    label="A/C Balance ＄",
    value=5 ,
    delta=7,
)

kpi4.metric(
    label="A/C Balance ＄",
    value=5 ,
    delta=7,
)
# Crear la figura con Plotly
fig_col1, fig_col2 = st.columns(2)

with fig_col1:
    st.markdown("### First Chart")
    fig1 = go.Figure(data=[
        go.Bar(name='Overall 1', x=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 1]['category'], y=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 1]['count']),
        go.Bar(name='Overall 2', x=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 2]['category'], y=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 2]['count']),
        go.Bar(name='Overall 3', x=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 3]['category'], y=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 3]['count']),
        go.Bar(name='Overall 4', x=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 4]['category'], y=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 4]['count']),
        go.Bar(name='Overall 5', x=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 5]['category'], y=count_reviews_per_category_and_overall[count_reviews_per_category_and_overall['overall'] == 5]['count']),
    ])

    # Actualizar el diseño del gráfico
    fig1.update_layout(barmode='group', xaxis_title='Categoría', yaxis_title='Conteo',
                    title='Conteo por Categoría y Overall', legend_title='Overall')

    # Mostrar el gráfico en Streamlit
    st.write(fig1)

with fig_col2:
    st.markdown("### Second Chart")
    fig2 = px.bar(count_reviews_per_category_and_overall, x='count', y='category', orientation='h', color='overall', 
                labels={'count': 'Conteo', 'category': 'Categoría'},
                title='Conteo por Categoría (Inverso)',
                height=500, color_discrete_sequence=px.colors.qualitative.Plotly[::-1])
    st.write(fig2)



st.title(page_title)
st.text("avg_words_per_review")
st.dataframe(avg_words_per_review)
st.text("count_reviews_per_category")
st.dataframe(count_reviews_per_category)
st.text("count_reviews_per_category_and_overall")
st.dataframe(count_reviews_per_category_and_overall)
st.text("count_reviews_per_overall")
st.dataframe(count_reviews_per_overall)
st.text("count_reviews_per_product_per_category")
st.dataframe(count_reviews_per_product_per_category)
st.text("count_similar_products_between_categories")
st.dataframe(count_similar_products_between_categories)
st.text("count_verified_reviews_per_category")
st.dataframe(count_verified_reviews_per_category)
st.text("distribution_of_description")
st.dataframe(distribution_of_description)
st.text("distrobution_images")
st.dataframe(distrobution_images)
st.text("mean_ratings_per_asin")
st.dataframe(mean_ratings_per_asin)
st.text("overall_distribution_per_product")
st.dataframe(overall_distribution_per_product)
st.text("price_comparison_between_categories")
st.dataframe(price_comparison_between_categories)
st.text("similar_items_per_asin")
st.dataframe(similar_items_per_asin)
st.text("total_products_per_brand")
st.dataframe(total_products_per_brand)
st.text("total_products_per_category")
st.dataframe(total_products_per_category)
st.text("total_reviews_per_reviewerid")
st.dataframe(total_reviews_per_reviewerid)
st.text("verified_distribution")
st.dataframe(verified_distribution)
st.text("vote_distribution")
st.dataframe(vote_distribution)
