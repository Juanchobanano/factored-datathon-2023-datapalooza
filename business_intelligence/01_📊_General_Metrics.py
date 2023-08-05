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

st.title("Unleash the Magic of Data Visualization: An Exciting Journey Awaits!")
st.write("Welcome to our platform for data visualization and analysis of products. Here you will find detailed data presented visually and accessibly, enabling you to conduct informed analyses. Explore our interactive visualizations to understand product behavior in the market and customer preferences. Whether you're a seller, entrepreneur, or simply someone interested in products, we provide the necessary information for making successful decisions. Empower your sales and maximize your success with our data! Start your data analysis journey now.")


# Selector para cambiar entre DataFrames
opciones = ['Total products per category', 'Count reviews per category', 'Price comparison between categories']
# Crear una lista con los valores de los KPIs
values = [
    total_products_per_category['total_products'][0], # 0
    total_products_per_category['total_products'][1],
    count_reviews_per_category['count'][0],
    count_reviews_per_category['count'][1],
    price_comparison_between_categories['avg_price'][0],
    price_comparison_between_categories['avg_price'][1] # 5
]
labels = ["Sofware 🖥️", "Video Games 🎮"]
# Colores personalizados para las barras



#-----------------------------------------------------------------

fig_pie = px.pie(total_products_per_brand.head(5), values='total_products', names='brand', title='Total Products per Brand')
fig_bar = go.Figure(
    data=[go.Bar(x=labels, y=[values[0], values[1]], marker=dict(color=['blue','green']))],
    layout=go.Layout(
        title="Total Products per Category",
        xaxis=dict(title="Category"),
        yaxis=dict(title="Total Products")
    )
)
col1, col2 = st.columns(2)

col1.plotly_chart(fig_bar, use_container_width=True)
col2.plotly_chart(fig_pie, use_container_width=True )

#-------------------------------------------------------------------
st.text("mean_ratings_per_asin")
fig_scatter = px.scatter(mean_ratings_per_asin, x='avg_rating', size='count', text='count',
                 labels={'asin': 'ASIN', 'avg_rating': 'Average Rating'},
                 title='Product Ranking by Average Rating')

st.text("count_reviews_per_category_and_overall")
# Create the Plotly bar chart
fig_bar = px.bar(count_reviews_per_category_and_overall, x='category', y='count', color='overall',
             labels={'category': 'Category', 'count': 'Count', 'overall': 'Overall Rating'},
             title='Count of Reviews per Category and Overall Rating')


col1, col2 = st.columns(2)


col1.plotly_chart(fig_bar, use_container_width=True)
col2.plotly_chart(fig_scatter, use_container_width=True )

#------------------------------------------------------------------------

fig1 = go.Figure([go.Bar(x=labels, y=values, marker=dict(color=['orange', 'purple']))])

# Actualizar el título y el nombre de los ejes
fig1.update_layout(
    title="Average Prices per Category",
    xaxis_title="Category",
    yaxis_title="Avg price"
)

fig2 = px.bar(count_reviews_per_overall, x='overall', y='count',
             labels={'overall': 'Overall Rating', 'count': 'Count'},
             title='Count of Reviews per Overall Rating')


col1, col2 = st.columns(2)

col1.plotly_chart(fig1, use_container_width=True)
col2.plotly_chart(fig2, use_container_width=True )


fig3 = px.bar(count_similar_products_between_categories, x='category', y=['also_buy_count', 'also_view_count'],
             labels={'category': 'Category', 'value': 'Count', 'variable': 'Type'},
             title='Also Buy and Also View Count per Category')
st.plotly_chart(fig3, use_container_width=True)