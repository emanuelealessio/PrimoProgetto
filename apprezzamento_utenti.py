from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse

# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Create the Spark session
spark = SparkSession.builder.appName("ApprezzamentoUtenti").getOrCreate()

# Read the CSV file
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

# Select the required columns
df_selected = df.select("UserId", "HelpfulnessNumerator", "HelpfulnessDenominator")

# Calculate the apprezzamento as the average of HelpfulnessNumerator / HelpfulnessDenominator
df_apprezzamento = df_selected.withColumn("Apprezzamento", col("HelpfulnessNumerator") / col("HelpfulnessDenominator"))

# Group by UserId and calculate the average apprezzamento
df_media_apprezzamento = df_apprezzamento.groupBy("UserId").avg("Apprezzamento")

# Order by apprezzamento in descending order
df_sorted = df_media_apprezzamento.orderBy("avg(Apprezzamento)", ascending=False)

# Save the sorted dataframe to the output path
df_sorted.write.csv(output_filepath, header=True)

# Mostra i primi 20 utenti con l'apprezzamento più alto
df_sorted.show(20)

# Stop the Spark session
spark.stop()

