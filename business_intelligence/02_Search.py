import pandas as pd
import numpy as np
import streamlit as st
import pandas as pd
from utils.load_data import load_multiple_data
from operator import itemgetter
import altair as alt
from streamlit_agraph import agraph, Node, Edge, Config, ConfigBuilder
import constants as ct

st.set_page_config(page_title="Products Search", page_icon="")
products_graph = pd.read_parquet("graph.parquet")
amazon_reviews = pd.read_parquet("amazon_reviews.parquet")
amazon_metadata = pd.read_parquet("amazon_metadata.parquet")
products_graph.columns = ["src", "dst", "label", "weight"]

st.title("🔍 Products Search")

# Searh bar

product = st.text_input("Search products by title", value = "Nintendo 64")

product = "B00005YDIC"

amazon_metadata = amazon_metadata[amazon_metadata.asin == product]
amazon_reviews = amazon_reviews[amazon_reviews.asin == product]
# Filter the dataframe using masks
#m1 = df["Autor"].str.contains(text_search)
#m2 = df["Título"].str.contains(text_search)
#df_search = df[m1 | m2]
st.dataframe(amazon_metadata)
st.dataframe(amazon_reviews)

#----
st.markdown("## Nintendo 64")
col1, col2 = st.columns(2)
with col1: 
    st.metric(label = "Degree Centrality", value = 0.7)
with col2: 
    st.metric(label = "Betweenness Centrality", value = 0.6)

#----




g1 = products_graph[products_graph['src'] == product]
result = pd.merge(g1[['dst']], products_graph[['src', 'dst', 'label', 'weight']], left_on='dst', right_on='dst', suffixes=('', '_g2'))
# Step 3: Join g2 and g3
#result = pd.merge(result[['src', 'dst', 'label', 'weight']], products_graph[['src', 'dst', 'label', 'weight']], left_on='src', right_on='src', suffixes=('_g2', ''))

# Select the desired columns from the final result
result = result[['src', 'dst', 'label', 'weight']]
#result = result[result.label != 'HAS_REVIEW']
#result = result[result.label != 'HAS_CATEGORY']

main = result[(result.src == product) | (result.label == 'HAS_REVIEW')]# & (result.src == product))]
not_main = result[(result.src != product) & (result.label != 'HAS_REVIEW')]
not_main = not_main.sample(n = 200)

#print("length main: ", len(main))
#print("length not main: ", len(not_main))

result = pd.concat([main, not_main])

#print("Length result: ", len(result))



# Display the final result
communities__graph = result

#communities__graph = communities__graph[communities__graph.community_id == community_number]
src = communities__graph["src"].to_frame()

src["label"] = "asin"
dst = communities__graph["dst"].to_frame()
dst["label"] = communities__graph["label"].apply(lambda z: ct.dicc[z])
dst.columns = ["src", "label"]
nodes = pd.concat([src, dst]).reset_index(drop = True)
nodes = nodes.drop_duplicates(subset=["src"])

#print(list(communities__graph.itertuples())[0:4])

nodes_results = list()
for row in nodes.itertuples():
    if row.src == product:
        node = Node(
            id = row.src, 
            title = row.src, 
            image = ct.node_image[row.label], 
            shape = "circularImage", 
            size = 50, 
            border = 50, 
            color = "yellow", 
            shadow = True, 
            label = row.src, 
            font = {}
        )
    else:
        node = Node(id = row.src, title = row.src, image = ct.node_image[row.label], shape = "circularImage", size = ct.size_image[row.label])
    nodes_results.append(node)

#nodes = [Node(id = row.src, title = row.src, image = ct.node_image[row.label], shape = "circularImage", size = ct.size_image[row.label]) for row in nodes.itertuples()] 
edges = [Edge(source = row.src, label = row.label, target = row.dst, font = {"size": ct.edge_font_size[row.label]}) for row in communities__graph.itertuples()]

config = Config(
                width = 750, #1024,
                height = 750, #750,
                directed=True, 
                physics=True, 
                hierarchical=False,
                link = {"labelProperty": "HAS_PHONE", "renderLabel": True},
                highlightColor = "#58FF33",
                #collapsible=True,
                node = {"labelProperty": "label"}, 
                #maxZoom=2, 
                minZoom=0.1,
                #initialZoom=1, 
                solver = "forceAtlas2Based",
                )

return_value = agraph(nodes=nodes_results, 
                    edges=edges, 
                    config=config)

print(return_value)

