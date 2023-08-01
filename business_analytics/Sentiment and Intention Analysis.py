# Databricks notebook source
import openai 
from openai.embeddings_utils import cosine_similarity, get_embedding

openai.api_key = "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"

# COMMAND ----------

sentiment_labels = ["Positive", "Neutral", "Negative"]
intention_labels = ["Expressing Satisfaction", "Voicing Dissatisfaction", "Seeking Support", "Providing Feedback", "Comparing Alternatives", "Promoting or Critiquing a Brand", "Sharing Tips and Tricks", "Expressing Brand Loyalty", "Warning about Potential Issues", "Seeking Validation"]

# COMMAND ----------

def get_embedding(text, model="text-embedding-ada-002"):
   text = text.replace("\n", " ")
   return openai.Embedding.create(input = [text], model=model)['data'][0]['embedding']

# COMMAND ----------

model = "text-embedding-ada-002"
model = "text-search-babbage-doc-001"

# COMMAND ----------

sentiment_label_embeddings = [get_embedding(label, model=model) for label in sentiment_labels]
intention_label_embeddings = [get_embedding(label, model=model) for label in intention_labels]

# COMMAND ----------

sentiment_dicc = {}
intention_dicc = {}

for i in range(len(sentiment_labels)):
    sentiment_dicc[sentiment_labels[i]] = sentiment_label_embeddings[i]
for i in range(len(intention_labels)):
    intention_dicc[intention_labels[i]] = intention_label_embeddings[i]

# COMMAND ----------

def label_score(review_embedding, labels_dicc):
    similarities = {}
    for key in labels_dicc.keys():
        similarities[key] = cosine_similarity(review_embedding, labels_dicc[key])
    return similarities

# COMMAND ----------

#review = """I ordered the same slacks - in the same size and style that i purchased from Haggar earlier - the new slacks showed up and where to small - the numbers on the label from Haggar where both the same except  one was made in China and the other in Bangladesh - The Bangladesh slacks where to small - maybe they use a different measuring  tape in Bangladesh. Had to return."""
#review = """Can't say enough about the fine quality, fit and comfort of these pants!  However, and this is really not a problem....I purchased two pair in size 32X34 and both were clearly marked as so but one pair actually has a 35" inseam.  As I said, it's insignificant....but, thanks Haggar!....In this day and age getting just a little more at the great price offered, in my opinion, is a bonus!  Am lovin' these pants!"""
#review = """I love this game so much. When I was a kid I had the second one. And when I was in middle school I saw what this first one had to offer and had wanted it since. I got this game my senior year and I've been addicted to playing these two for the longest. It's so challenging and you have to make it to the goal before time runs out. It's a fun game to play by yourself or with others. I'm so happy to have both of these now. This is a must get game :D
#"""
review = """I ordered 3 pairs. Only one lasted more that a couple of weeks. I even ordered on size larger in case the fit wasn't right. The larger pair ripped in the crotch within 3 wearings. The other one, the button broke, (not just fell off) shattered while I was giving a lecture. (Very embarrassing!) I won't wear the last pair because I don't trust them. None of the pants were tight, they were actually very comfortable. I guess they just don't stand up to the rigorous life of a junior high school teacher. I don't usually take the time to write reviews but I live in Japan, clothes that fit are extremely hard to come by. Losing those pants was a huge blow to my wallet."""

# COMMAND ----------

review_embedding = get_embedding(review, model = model)

# COMMAND ----------

list_ = list(label_score(review_embedding, sentiment_dicc).items())
list_.sort(key = lambda z: z[1], reverse = True)
list_

# COMMAND ----------

list_ = list(label_score(review_embedding, intention_dicc).items())
list_.sort(key = lambda z: z[1], reverse = True)
#list_ = [x[0] for x in list_]
list_

# COMMAND ----------

((10000000 * (0.35+0.59))/60)/60

# COMMAND ----------

import concurrent.futures

def compute_similarity(x, y):
    return 

with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(compute_similarity, a)
