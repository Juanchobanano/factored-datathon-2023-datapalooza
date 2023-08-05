data_list = [
    {"share": "share__hackaton__streamlit", "schema": "gold", "table": "products_graph"},
    {"share": "share__hackaton__streamlit", "schema": "silver", "table": "amazon_reviews_selected"},
    {"share": "share__hackaton__streamlit", "schema": "silver", "table": "amazon_metadata_silver_selected"},
    {"share": "share__hackaton__streamlit", "schema": "gold", "table": "degree_centrality_normalized"},
    {"share": "share__hackaton__streamlit", "schema": "gold", "table": "betweenes_centrality"},
]
data = load_multiple_data(data_list)
(
    products_graph,
    product_reviews,
    product_metadata,
    degree_centrality,
    betweenness_centrality,
) = itemgetter(
    "products_graph",
    "amazon_reviews_selected",
    "amazon_metadata_silver_selected",
    "degree_centrality_normalized",
    "betweenes_centrality",
)(data)