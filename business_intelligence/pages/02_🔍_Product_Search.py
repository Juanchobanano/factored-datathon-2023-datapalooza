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
st.set_page_config(page_title="Product Search", page_icon="🔍", layout="wide")
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

# load data
#products_graph = pd.read_parquet("./data/graph.parquet")
#product_reviews = pd.read_parquet("./data/amazon_reviews.parquet")
#product_metadata = pd.read_parquet("./data/amazon_metadata.parquet")
#degree_centrality = pd.read_parquet("./data/degree_centrality.parquet")
#betweenness_centrality = pd.read_parquet("./data/betweenness_centrality.parquet")

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
    products_reviews,
    products_metadata,
    degree_centrality,
    betweenness_centrality,
) = itemgetter(
    "products_graph",
    "amazon_reviews_selected",
    "amazon_metadata_silver_selected",
    "degree_centrality_normalized",
    "betweenes_centrality",
)(data)

# main title
st.title("🔍 Product Search")

# Search bar
tab1, tab2 = st.tabs(["🧠 Semantic Search", "🌟 Most popular products"])

with tab1: 

    st.markdown("Utilize semantic search to find products effortlessly. Explore metrics 📊 of the selected product and its subgraph 🌐. Streamline your search experience! 🚀")
    st.caption("Start by trying something like 'Nintendo 64'")
    query_text = st.text_input("🛍️ Search a product")
    max_results = st.slider(
        "🎛️ Select the maximum number of elements you want to retrieve", 
        min_value = 5, 
        max_value = 30, 
        step = 5, 
        key = "slider_1"
    )
    st.divider()

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
        
        # Get product asin
        product_asin = results_metadata.iloc[product_idx]["asin"]

        # Product Information
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader("🏷️ Title")
            #title = results_metadata["title"].tolist()
            title = results_metadata.iloc[product_idx]["title"] #.values[0]
            st.markdown(title)
        with col2:
            st.subheader("🏢 Brand")
            #brand = results_metadata["brand"].tolist()
            brand = results_metadata.iloc[product_idx]["brand"] #.values[0]
            st.markdown(brand)
        with col3:
            st.subheader("💵 Mean Price")
            price = results_metadata.iloc[product_idx]["mean_price"] #.values[0]
            if price != "Not available":
                price = round(float(price), 2)
            st.markdown(f"USD {price}")


        product_metadata = products_metadata[products_metadata.asin == product_asin]

        product_reviews = products_reviews[products_reviews.asin == product_asin]

        # show additional product information
        st.subheader("📚 Additional Product Information")
        additional_info = product_metadata[["description", "feature", "category"]]
        additional_info.columns = ["Product Description", "Features", "Subcategories"]
        st.dataframe(additional_info, use_container_width=True, hide_index=True)

        # show product reviews
        st.subheader("📝 Product Reviews")
        show_amazon_reviews = product_reviews[
            ["reviewID", "date", "reviewerName", "summary", "reviewText", "verified", "vote"]
        ]
        st.dataframe(show_amazon_reviews, use_container_width=True, hide_index=True)

        # Show product subgraph
        st.subheader("🏘️ Product Neighborhood")

        # Show degree and betweenness centrality metrics
        col1, col2 = st.columns(2)
        with col1:
            try:
                value = degree_centrality[degree_centrality.node == product_asin]["degree_centrality_normalized"].values[0]
                value = round(value, 3)
            except:
                value = "Not available"
            st.metric(label="Product Local Degree Centrality (from 0 to 1)", value=value)
            st.caption(
                "Measure how popular a product is in the overall of products within the Video Games and Software categories, helping companies to allocate marketing budgets more effectively and focus on products with high customer engagement."
            )
        with col2:
            try:
                value = betweenness_centrality[betweenness_centrality.node == product_asin][
                    "betweenes_centrality_normalized"
                ].values[0]
                value = round(value, 3)
            except:
                value = "Not available"
            st.metric(label="Product Local Betweenness Centrality (from 0 to 1)", value=value)
            st.caption(
                "Measures how influential a product is within its neighborhood, helping companies target their marketing efforts more effectively. Collaborating with influential brands or featuring influential products can boost exposure and sales."
            )

        # Get graph
        g1 = products_graph[products_graph["src"] == product_asin]
        result = pd.merge(
            g1[["dst"]], products_graph[["src", "dst", f"label", "weight"]], left_on="dst", right_on="dst", suffixes=("", "_g2")
        )
        result = result[["src", "dst", "label", "weight"]]
        main = result[(result.src == product_asin) | (result.label == "HAS_REVIEW")]  # & (result.src == product))]
        not_main = result[(result.src != product_asin) & (result.label != "HAS_REVIEW")]
        if len(not_main) != 0:
            if len(not_main) > 200:
                not_main = not_main.sample(n=200)
            result = pd.concat([main, not_main])
        else:
            result = main
        communities__graph = result
        src = communities__graph["src"].to_frame()
        src["label"] = "asin"
        dst = communities__graph["dst"].to_frame()
        dst["label"] = communities__graph["label"].apply(lambda z: ct.dicc[z])
        dst.columns = ["src", "label"]
        nodes = pd.concat([src, dst]).reset_index(drop=True)
        nodes = nodes.drop_duplicates(subset=["src"])

        # Prepare graph.
        nodes_results = list()
        for row in nodes.itertuples():
            if row.src == product_asin:
                node = Node(
                    id=row.src,
                    title=row.src,
                    image=ct.node_image[row.label],
                    shape="circularImage",
                    size=50,
                    border=50,
                    color="yellow",
                    shadow=True,
                    label=row.src,
                    font={},
                )
            else:
                node = Node(
                    id=row.src,
                    title=row.src,
                    image=ct.node_image[row.label],
                    shape="circularImage",
                    size=ct.size_image[row.label],
                )
            nodes_results.append(node)

        edges = [
            Edge(source=row.src, label=row.label, target=row.dst, font={"size": ct.edge_font_size[row.label]})
            for row in communities__graph.itertuples()
        ]

        config = Config(
            width=750,  # 1024,
            height=750,  # 750,
            directed=True,
            physics=True,
            hierarchical=False,
            link={"labelProperty": "HAS_PHONE", "renderLabel": True},
            highlightColor="#58FF33",
            # collapsible=True,
            node={"labelProperty": "label"},
            # maxZoom=2,
            minZoom=0.1,
            # initialZoom=1,
            solver="forceAtlas2Based",
        )

        # Show graph.
        return_value = agraph(nodes=nodes_results, edges=edges, config=config)

with tab2:
    st.markdown("Discover top products using degree graph centrality. Explore metrics 📊 of the chosen product and its subgraph 🌐. Uncover trends! 🚀")
    max_results = st.slider(
        "🎛️ Select the maximum number of elements you want to retrieve", 
        min_value = 5, 
        max_value = 100, 
        step = 5, 
        key = "slider_2"
    )
    st.divider()

    degree_centrality = degree_centrality.sort_values(by = "degree_centrality_normalized", ascending=False)
    results_asin = list(degree_centrality.iloc[0:max_results]["node"].values)

    # Filter results metadata and reviews
    results_asin_mask = products_metadata["asin"].isin(results_asin)
    results_metadata = products_metadata[results_asin_mask].reset_index(drop=True).fillna("Not available")

    # Display query results
    options = list(results_metadata["title"].values)
    product_idx = st.selectbox(f"📊 Top {max_results} products", range(len(options)), format_func=lambda x: options[x], key = "bar2")
    
    # Get product asin
    product_asin = results_metadata.loc[product_idx, "asin"]
    st.write(product_asin)

    # Product Information
    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("🏷️ Title")
        #title = results_metadata["title"].tolist()
        title = results_metadata.loc[product_idx, "title"] #.values[0]
        st.markdown(title)
    with col2:
        st.subheader("🏢 Brand")
        #brand = results_metadata["brand"].tolist()
        brand = results_metadata.loc[product_idx, "brand"] #.values[0]
        st.markdown(brand)
    with col3:
        st.subheader("💵 Mean Price")
        price = results_metadata.loc[product_idx, "mean_price"] #.values[0]
        if price != "Not available":
            price = round(float(price), 2)
        st.markdown(f"USD {price}")

    

    product_metadata = products_metadata[products_metadata.asin == product_asin]
    product_reviews = products_reviews[products_reviews.asin == product_asin]

    # show additional product information
    st.subheader("📚 Additional Product Information")
    additional_info = product_metadata[["description", "feature", "category"]]
    additional_info.columns = ["Product Description", "Features", "Subcategories"]
    st.dataframe(additional_info, use_container_width=True, hide_index=True)

    # show product reviews
    st.subheader("📝 Product Reviews")
    show_amazon_reviews = product_reviews[
        ["reviewID", "date", "reviewerName", "summary", "reviewText", "verified", "vote"]
    ]
    st.dataframe(show_amazon_reviews, use_container_width=True, hide_index=True)

    # Show product subgraph
    st.subheader("🏘️ Product Neighborhood")

    # Show degree and betweenness centrality metrics
    col1, col2 = st.columns(2)
    with col1:
        try:
            value = degree_centrality[degree_centrality.node == product_asin]["degree_centrality_normalized"].values[0]
            value = round(value, 3)
        except:
            value = "Not available"
        st.metric(label="Product Local Degree Centrality (from 0 to 1)", value=value)
        st.caption(
            "Measure how popular a product is in the overall of products within the Video Games and Software categories, helping companies to allocate marketing budgets more effectively and focus on products with high customer engagement."
        )
    with col2:
        try:
            value = betweenness_centrality[betweenness_centrality.node == product_asin][
                "betweenes_centrality_normalized"
            ].values[0]
            value = round(value, 3)
        except:
            value = "Not available"
        st.metric(label="Product Local Betweenness Centrality (from 0 to 1)", value=value)
        st.caption(
            "Measures how influential a product is within its neighborhood, helping companies target their marketing efforts more effectively. Collaborating with influential brands or featuring influential products can boost exposure and sales."
        )

    # Get graph
    g1 = products_graph[products_graph["src"] == product_asin]
    result = pd.merge(
        g1[["dst"]], products_graph[["src", "dst", f"label", "weight"]], left_on="dst", right_on="dst", suffixes=("", "_g2")
    )
    result = result[["src", "dst", "label", "weight"]]
    main = result[(result.src == product_asin) | (result.label == "HAS_REVIEW")]  # & (result.src == product))]
    not_main = result[(result.src != product_asin) & (result.label != "HAS_REVIEW")]
    if len(not_main) != 0:
        if len(not_main) > 200:
            not_main = not_main.sample(n=200)
        result = pd.concat([main, not_main])
    else:
        result = main
    communities__graph = result
    src = communities__graph["src"].to_frame()
    src["label"] = "asin"
    dst = communities__graph["dst"].to_frame()
    dst["label"] = communities__graph["label"].apply(lambda z: ct.dicc[z])
    dst.columns = ["src", "label"]
    nodes = pd.concat([src, dst]).reset_index(drop=True)
    nodes = nodes.drop_duplicates(subset=["src"])

    # Prepare graph.
    nodes_results = list()
    for row in nodes.itertuples():
        if row.src == product_asin:
            node = Node(
                id=row.src,
                title=row.src,
                image=ct.node_image[row.label],
                shape="circularImage",
                size=50,
                border=50,
                color="yellow",
                shadow=True,
                label=row.src,
                font={},
            )
        else:
            node = Node(
                id=row.src,
                title=row.src,
                image=ct.node_image[row.label],
                shape="circularImage",
                size=ct.size_image[row.label],
            )
        nodes_results.append(node)

    edges = [
        Edge(source=row.src, label=row.label, target=row.dst, font={"size": ct.edge_font_size[row.label]})
        for row in communities__graph.itertuples()
    ]

    config = Config(
        width=1024,  # 1024,
        height=1024,  # 750,
        directed=True,
        physics=True,
        hierarchical=False,
        link={"labelProperty": "HAS_PHONE", "renderLabel": True},
        highlightColor="#58FF33",
        # collapsible=True,
        node={"labelProperty": "label"},
        # maxZoom=2,
        minZoom=0.1,
        # initialZoom=1,
        solver="forceAtlas2Based",
    )

    # Show graph.
    return_value = agraph(nodes=nodes_results, edges=edges, config=config)