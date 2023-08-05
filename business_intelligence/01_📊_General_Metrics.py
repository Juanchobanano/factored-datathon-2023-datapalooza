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

st.write("total_products_per_category")

# create three columns
kpi1, kpi2= st.columns(2)
# fill in those three columns with respective metrics or KPIs
kpi1.metric(
    label=labels[0],
    value=values[0],
)
kpi2.metric(
    label=labels[1],
    value=values[1],
)
# Crear la figura de la gráfica de barras
fig = go.Figure([go.Bar(x=labels, y=[values[0], values[1]], marker=dict(color=['blue','green']))])
# Actualizar el título y el nombre de los ejes
fig.update_layout(
    xaxis_title="Category",
    yaxis_title="Total Products"
)
st.plotly_chart(fig, use_container_width=True)

#--------------------------------------------------------------------------------------------------------------------
st.write("count_reviews_per_category")
# create three columns
kpi1, kpi2= st.columns(2)
# fill in those three columns with respective metrics or KPIs
kpi1.metric(
    label=labels[0],
    value=values[2],
)
kpi2.metric(
    label=labels[1],
    value=values[3],
)
# Crear la figura de la gráfica de barras
fig = go.Figure([go.Bar(x=labels, y=[values[2], values[3]], marker=dict(color=['yellow','pink']))])
# Actualizar el título y el nombre de los ejes
fig.update_layout(
    xaxis_title="Category",
    yaxis_title="Total Reviews"
)
st.plotly_chart(fig, use_container_width=True)
#--------------------------------------------------------------------------------------------------------------------
st.write("price_comparison_between_categories")

# create three columns
kpi1, kpi2= st.columns(2)
# fill in those three columns with respective metrics or KPIs
kpi1.metric(
    label=labels[0],
    value=round(values[4],2),
)
kpi2.metric(
    label=labels[1],
    value=round(values[5],2),
)
# Crear la figura de la gráfica de barras
fig = go.Figure([go.Bar(x=labels, y=[values[4], values[5]], marker=dict(color=['orange','purple']))])
# Actualizar el título y el nombre de los ejes
fig.update_layout(
    xaxis_title="Category",
    yaxis_title="Avg price"
)
st.plotly_chart(fig, use_container_width=True)
#------------------------------------------------------------------------------------------

st.text("count_reviews_per_category_and_overall")
# Create the Plotly bar chart
fig = px.bar(count_reviews_per_category_and_overall, x='category', y='count', color='overall',
             labels={'category': 'Category', 'count': 'Count', 'overall': 'Overall Rating'},
             title='Count of Reviews per Category and Overall Rating')

# Display the chart using Streamlit
st.plotly_chart(fig, use_container_width=True)


#------------------------------------------------------------------------------------------
st.text("count_reviews_per_overall")
fig = px.bar(count_reviews_per_overall, x='overall', y='count',
             labels={'overall': 'Overall Rating', 'count': 'Count'},
             title='Count of Reviews per Overall Rating')

# Mostrar la gráfica
st.plotly_chart(fig)
#------------------------------------------------------------------------------------------
st.text("count_similar_products_between_categories")
# Crear la figura del gráfico de barras con Plotly Express
fig = px.bar(count_similar_products_between_categories, x='category', y=['also_buy_count', 'also_view_count'],
             labels={'category': 'Category', 'value': 'Count', 'variable': 'Type'},
             title='Also Buy and Also View Count per Category')

# Mostrar la gráfica en Streamlit
st.plotly_chart(fig)
#------------------------------------------------------------------------------------------
st.text("count_verified_reviews_per_category")

# Crear la figura del gráfico de barras con Plotly Express
fig = px.bar(count_verified_reviews_per_category, x='category', y=['verified_count', 'unverified_count'],
             labels={'category': 'Category', 'value': 'Count', 'variable': 'Type'},
             title='Verified and Unverified Count per Category')


# Mostrar la gráfica en Streamlit
st.plotly_chart(fig)

#------------------------------------------------------------------
st.text("mean_ratings_per_asin")
fig = px.scatter(mean_ratings_per_asin, x='avg_rating', size='count', text='count',
                 labels={'asin': 'ASIN', 'avg_rating': 'Average Rating'},
                 title='Product Ranking by Average Rating')
st.plotly_chart(fig, use_container_width=True)

#------------------------------------------------------------------
st.text("total_products_per_brand")
fig = px.bar(total_products_per_brand.head(5), x='brand', y='total_products',
             labels={'brand': 'Brand', 'total_products': 'Total Products'},
             title='Total Products per Brand')
st.plotly_chart(fig, use_container_width= True )


