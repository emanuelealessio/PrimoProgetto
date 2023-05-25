-- Leggi il dataset Amazon Fine Food Reviews in Hive
CREATE TABLE IF NOT EXISTS amazon_reviews (
  ProductId STRING,
  Score INT,
  Time INT,
  Text STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'Reviews_processed.csv' INTO TABLE amazon_reviews;


-- Converti il campo Time da Unix Time a formato data
SELECT year(from_unixtime(Time)) AS year, ProductId, Text
FROM amazon_reviews;

CREATE TABLE IF NOT EXISTS reviews_counts (
  year INT,
  ProductId STRING,
  review_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Raggruppa i dati per anno e ProductId e conta il numero di recensioni per ogni coppia anno-ProductId
INSERT OVERWRITE TABLE reviews_counts
SELECT year, ProductId, count(*) AS review_count
FROM (
  SELECT year(from_unixtime(Time)) AS year, ProductId, Text
  FROM amazon_reviews
) AS temp
GROUP BY year, ProductId;

CREATE TABLE IF NOT EXISTS top_products (
year INT,
ProductId STRING,
review_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Ordina i dati in base al numero di recensioni in ordine decrescente e seleziona i primi 10 prodotti per ogni anno
INSERT OVERWRITE TABLE top_products
SELECT year, ProductId, review_count
FROM (
  SELECT year, ProductId, review_count,
         ROW_NUMBER() OVER (PARTITION BY year ORDER BY review_count DESC) AS row_num
  FROM reviews_counts
) AS temp
WHERE row_num <= 10;

CREATE TABLE IF NOT EXISTS words_counts (
year INT,
ProductId STRING,
word STRING,
count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Per ogni prodotto selezionato, estrai il testo delle recensioni e dividi le parole
INSERT OVERWRITE TABLE words_counts
SELECT year, ProductId, word, count(*) AS count
FROM (
  SELECT year(from_unixtime(Time)) AS year, ProductId, Text
  FROM amazon_reviews
) AS temp1
LATERAL VIEW explode(split(Text, ' ')) temp2 AS word

-- Filtra le parole per quelle con almeno 4 caratteri
WHERE length(word) >= 4

-- Conta il numero di occorrenze di ogni parola e ordina le parole in base al numero di occorrenze in ordine decrescente
GROUP BY year, ProductId, word
ORDER BY year, ProductId, count DESC;

-- Seleziona le prime 5 parole per ogni prodotto e stampale insieme al numero di occorrenze
SELECT year, ProductId, word, count
FROM (
  SELECT year, ProductId, word, count,
         ROW_NUMBER() OVER (PARTITION BY year, ProductId ORDER BY count DESC) AS row_num
  FROM words_counts
) AS temp
WHERE row_num <= 5;

