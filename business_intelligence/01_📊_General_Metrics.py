import streamlit as st
import pandas as pd
import utils
from utils.load_data import load_multiple_data
from operator import itemgetter
import altair as alt

import plotly.express as px
import plotly.graph_objects as go


import time

page_title="General Metrics"


st.set_page_config(
    page_title, 
    page_icon="",
    layout= "wide",
    )



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



#-----------------------------------------------------------------

st.title('📈 Understanding the Importance of Knowing Your Data Product for Increased Sales')
st.markdown('## 📊 Data-Driven Decision Making')
st.write("Understanding your data product thoroughly is crucial for driving sales and business success in today's era. The market is increasingly moving towards a data-driven approach, and knowing how your customers perceive your product and their behavior in relation to it is a key factor in making informed strategic decisions. By comprehending the data behind your product, such as customer preferences, purchasing trends, and customer satisfaction, you can identify improvement opportunities and develop effective strategies to increase sales and enhance the customer experience.")

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
st.markdown('## 🌟 The Impact of Customer Reviews')
st.write("One essential metric every business owner should be aware of is the number of reviews their product receives. Customer reviews have a significant impact on the purchasing decisions of other potential buyers. The more positive reviews your product has, the more confidence it instills in consumers, making them more likely to make a purchase. Moreover, reviews can also provide valuable insights into which aspects of your product are appreciated by customers and which areas may need improvement. Knowing this information will allow you to adjust your marketing strategy and foster customer loyalty.")


# Create the Plotly bar chart
# Definir colores personalizados
color_map = {
    1: 'red',
    2: 'green',
    3: 'blue',
    4: 'purple',
    5: 'orange',
}

fig2 = px.bar(count_reviews_per_overall,
              x='overall',
              y='count',
              color='overall',
              color_discrete_map=color_map,  # Colores personalizados aquí
              labels={'overall': 'Overall Rating', 'count': 'Count'},
              title='Count of Reviews per Overall Rating')
# Gráfico de dispersión con tamaño de burbuja y colores personalizados
fig_bubble = px.scatter(count_reviews_per_category_and_overall,
                        x='category',
                        y='count',
                        size='overall',
                        color='overall',
                        color_continuous_scale=px.colors.sequential.Viridis,  # Cambiar el mapa de colores aquí
                        labels={'category': 'Category', 'count': 'Count', 'overall': 'Overall Rating'},
                        title='Count of Reviews per Category and Overall Rating')


col1, col2 = st.columns(2)


col1.plotly_chart(fig_bubble, use_container_width=True)
col2.plotly_chart(fig2, use_container_width=True )

#------------------------------------------------------------------------
st.markdown('## 🚀 Strategic Business Planning')
st.write("By collecting and analyzing data about your product and customer reviews, you will be well-positioned to devise an effective business strategy. In the realm of Product Search, having a detailed understanding of the data will enable you to thoroughly investigate which keywords, tags, or terms are most relevant to your product and appeal to your target audience. Identifying these essential keywords will improve the visibility of your product in online searches and, consequently, increase the chances of being found by potential customers. Additionally, understanding which aspects of your product stand out in reviews will allow you to optimize your marketing messages and highlight your strengths to attract more customers and differentiate yourself from the competition. In summary, knowing your data product will empower you to make intelligent decisions that drive your business's growth in an increasingly competitive market.")
fig_scatter = px.scatter(mean_ratings_per_asin, x='avg_rating', size='count', text='count',
                 labels={'asin': 'ASIN', 'avg_rating': 'Average Rating'},
                 title='Product Ranking by Average Rating')


fig1 = go.Figure([go.Bar(x=labels, y=[values[4],values[5]], marker=dict(color=['orange', 'purple']))])

# Actualizar el título y el nombre de los ejes
fig1.update_layout(
    title="Average Prices per Category",
    xaxis_title="Category",
    yaxis_title="Avg price"
)

col1, col2 = st.columns(2)
col1.plotly_chart(fig1, use_container_width=True)
col2.plotly_chart(fig_scatter, use_container_width=True )

