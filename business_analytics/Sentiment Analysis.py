# Databricks notebook source
# MAGIC %md
# MAGIC # Sentiment Analysis

# COMMAND ----------

from transformers import pipeline
from transformers import pipeline
sentiment_pipeline = pipeline("sentiment-analysis")
data = ["I love you", "I hate you"]
sentiment_pipeline(data)

# COMMAND ----------

text = """I can't help but laugh a little bit. LucasArts, once known for the very best in computer games, has within the past few years begun to decline in product quality and originality. Once upon a time they made actual non-Star Wars games too, ones that were really outstanding (Indiana Jones and the Fate of Atlantis, Sam and Max hit the Road, and The Dig come to mind), but as of late seem to be focusing almost entirely on the Star Wars motif itself. There's nothing wrong with that...since the release of SW Episodes 1 and 2, there's a much higher demand for Star Wars related games, and they're simply trying to fill in the demand.
  But so many of the games are just terrible! And Force Commander is a good one to pick out of the bunch to illustrate this fact. The idea behind it I'm sure is that the guys at LucasArts saw how popular StarCraft was, and knew that Warcraft III was coming. So why not beat the boys at Blizzard to the punch? Thus became FoCom.
  The plot is actually quite good....LucasArts never skimped on many games as far as that goes, and a great deal of effort went into the storytelling side of this game. You start out working for the Imperials, following the path of a certain Astromech droid who happens to have Death Star plans in it's memory banks. The graphics really aren't that bad either. The details on some of the units you have is quite good depending on what unit you're looking at.
  The problems with the game almost outweigh the good things though. The camera control can be likened to strapping a camcorder to Tarzan's head and having him swing over the battlefield on a vine. And you can never seem to focus just right on the units.
  But let's head straight to the single biggest gripe anyone who's played this game has. The music!...or should I say Muzak?
From the moment that extremely unique combination of classic John Williams gets blended with 2nd rate elevator music disco, you feel like screaming. The scary thing is that it grows on you as you play, and after 4-5 hours of it you'll start beebopping your head to certain tracks of the stuff.
  The game isn't overly difficult to play, but there's some imbalance to it. An earlier post said you can win solely with infantry...and he's right. You need only build those to win. But make sure to mass them before you do attack, as your transport shuttles will only deliver 6 of the guys at a time. AT-AT's are still fun to run around in though, I won't lie, although they do have a tendency to get quagmired easily.
  These are all overlookable problems in themselves, namely because of the price. You'll not find a better deal ..., and if you stick with it you'll find the game is fun, just a little hard to get into at first. Oh, just remember to turn off the music ;)"""

# COMMAND ----------

len(text)

# COMMAND ----------

sentiment_pipeline(text[0:2000])

# COMMAND ----------

from pyspark.sql.types import *
def function_sentiment_pipeline(doc):
    doc = doc[0:2000]
    try:
        return sentiment_pipeline(doc)[0]["label"]
    except:
        return None
udf_function_sentiment_pipeline = f.udf(lambda z: function_sentiment_pipeline(z), StringType())

# COMMAND ----------

function_sentiment_pipeline("i love you")

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE CATALOG products;
# MAGIC USE SCHEMA silver;
# MAGIC SHOW TABLES;

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

silver_reviews = spark.table("products.silver.amazon_reviews_selected")
print(silver_reviews.count())
silver_reviews = silver_reviews.withColumn("reviewID", f.sha1(f.col("reviewText"))).drop_duplicates(["reviewID"])
print(silver_reviews.count())
#silver_reviews.write.format("delta").mode("overwrite").saveAsTable("products.silver.amazon_reviews_silver")#.display()

# COMMAND ----------

silver_reviews.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable("products.silver.amazon_reviews_silver")#.display()

# COMMAND ----------



# COMMAND ----------

spark.table("products.silver.amazon_reviews_silver").count()

# COMMAND ----------

((1572122*0.17)/60)/60

# COMMAND ----------

silver_reviews = spark.table("products.silver.amazon_reviews_silver")
silver_reviews = silver_reviews.select("asin", "reviewerID", "date", "reviewID", "reviewText", "source").where(f.col("source") == "batch")#.display()
#silver_reviews = silver_reviews.limit(1000).withColumn("sentiment", udf_function_sentiment_pipeline(f.col("reviewText")))
#silver_reviews.display()

# COMMAND ----------

pandas_df = silver_reviews.select("reviewID", "reviewText").toPandas()

# COMMAND ----------

pandas_df.head()

# COMMAND ----------

info = pandas_df.to_dict("records")

# COMMAND ----------

for i in range(len(info)):
    print(data)
    break

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import *
def function_sentiment_pipeline(doc):
    doc = doc["reviewText"]
    doc = doc[0:2000]
    try:
        return sentiment_pipeline(doc)[0]["label"]
    except:
        return None

# COMMAND ----------

function_sentiment_pipeline(info[0])

# COMMAND ----------

import concurrent.futures

with concurrent.futures.ProcessPoolExecutor() as executor:
    future = executor.map(function_sentiment_pipeline, info[0:10])
    print(future.result())

# COMMAND ----------

info[0:10]

# COMMAND ----------

 with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(function_sentiment_pipeline, text = review) for review in info[0:10]]
        # Wait for futures to complete with a timeout
        completed, not_completed = concurrent.futures.wait(futures, timeout=30)

        return completed, not_completed

# COMMAND ----------

from pyspark.sql.types import *

def function_sentiment_pipeline(doc) -> str:
    try:
        doc = doc[0:2000]
        try:
            return sentiment_pipeline(doc)[0]["label"]
        except:
            return str(e)
    except Exception as e: 
        return str(e)

# COMMAND ----------

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(function_sentiment_pipeline, text = review["reviewText"]) for review in info[0:10]]
    # Wait for futures to complete with a timeout
    completed, not_completed = concurrent.futures.wait(futures, timeout=30)

# COMMAND ----------

completed

# COMMAND ----------

[function_sentiment_pipeline(review) for review in info[0:10]]

# COMMAND ----------

completed, not_completed = run_concurrent_get_embedding(info[0:10])
for future in completed:
    print(future)

# COMMAND ----------

completed

# COMMAND ----------

not_completed

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

((((8000000 * (0.35+0.59+0.19))/60)/60)/100)/24

# COMMAND ----------

import concurrent.futures

def compute_similarity(x, y):
    return 

with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(compute_similarity, a)

# COMMAND ----------

!python -m spacy download en_core_web_md

# COMMAND ----------

from transformers import pipeline

# COMMAND ----------

data = ["I love you", "I hate you"]
sentiment_pipeline(data)

# COMMAND ----------

from transformers import BertTokenizer, TFBertModel

tokenizer = BertTokenizer.from_pretrained('bert-base-cased')
model = TFBertModel.from_pretrained("bert-base-cased")
text_1 = "Replace me by any text you'd like."
encoded_text = tokenizer(text_1, return_tensors='tf')
output = model(encoded_text)

# COMMAND ----------

output

# COMMAND ----------

encoded_text = tokenizer(text_1, return_tensors='tf')
output = model(encoded_text)

# COMMAND ----------

vector = output.last_hidden_state.numpy()

# COMMAND ----------

vector.shape

# COMMAND ----------

vector

# COMMAND ----------

output[0]

# COMMAND ----------

#import libraries
from sentence_transformers import SentenceTransformer
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
#load model
model  = SentenceTransformer("bert-base-uncased")
# see the details of model with print
print(model)
# creating list of sentences
sentences_list = ["The dog barked loudly at the mailman.",
"The cat meowed incessantly until it was fed.",
"The horse neighed as it galloped across the field.",
"The cow mooed while grazing in the pasture.",
"The sheep bleated as it was herded into the pen."]
#takin embedding of list
embeddings =  model.encode(sentences_list)

# checking cosine similaries for all sentences
similarities = cosine_similarity(embeddings)
similarities
#calculation similaries between first and second sentences
similarities_pair = cosine_similarity(embeddings[0:1],embeddings[1:2])
similarities_pair
#calculation similarities between first sentence and others
similarities_first = cosine_similarity(embeddings[0:1],embeddings[1:])
similarities_first

# COMMAND ----------

embeddings[0:1].shape

# COMMAND ----------


