import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, length, year, col
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# create SparkSession
spark = SparkSession.builder.appName("AmazonReviews").getOrCreate()

# read input data into a DataFrame
reviews_df = spark.read.csv(input_filepath, header=True, inferSchema=True)

# select necessary columns and convert Time column from Unix time to year
reviews_df = reviews_df.select("ProductId", "Score", "Time", "Text") \
                       .withColumn("year", year(reviews_df.Time.cast("timestamp")))

# group reviews by year and ProductId and count number of reviews for each year-ProductId pair
reviews_counts_df = reviews_df.groupBy("year", "ProductId") \
                             .count() \
                             .withColumnRenamed("count", "review_count")

# window function to get the top 10 products with highest review count for each year
window_spec = Window.partitionBy("year").orderBy(reviews_counts_df.review_count.desc())
top_products_df = reviews_counts_df.select("*", rank().over(window_spec).alias("rank")).filter(col("rank") <= 10)

# explode Text column by splitting on whitespace and filter words with length >= 4
words_df = reviews_df.withColumn("word", explode(split(reviews_df.Text, " "))) \
                     .filter(length(col("word")) >= 4)

# group words by year, ProductId and word, and count number of occurrences for each year-ProductId-word triplet
words_counts_df = words_df.groupBy("year", "ProductId", "word") \
                          .count() \
                          .withColumnRenamed("count", "word_count")

# window function to get the top 5 most frequent words for each year-ProductId pair
window_spec = Window.partitionBy("year", "ProductId").orderBy(words_counts_df.word_count.desc())
top_words_df = words_counts_df.select("*", rank().over(window_spec).alias("rank")).filter(col("rank") <= 5)

# write output DataFrames to CSV files
top_products_df.write.csv(output_filepath + "/top_products", header=True, mode="overwrite")
top_words_df.write.csv(output_filepath + "/top_words", header=True, mode="overwrite")

top_products_df.show(20)
top_words_df.show(20)

# stop SparkSession
spark.stop()

# Il file CSV nella cartella "top_products" contiene 10 righe per ogni anno del dataset di recensioni Amazon, con all'interno il maggior numero di recensioni per ogni prodotto, ordinati in ordine decrescente per il numero di recensioni.

# Il file CSV nella cartella "top_words" contiene le prime 5 parole più frequenti per ogni anno e prodotto nel dataset di recensioni Amazon, insieme al conteggio di quante volte ogni parola appare per ogni anno e prodotto.
