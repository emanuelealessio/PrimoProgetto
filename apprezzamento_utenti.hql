-- Creazione della tabella Hive
CREATE TABLE apprezzamento_utenti (
  ProductId STRING,
  UserId STRING,
  HelpfulnessNumerator INT,
  HelpfulnessDenominator INT,
  Score INT
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;

-- Caricamento dei dati nella tabella
LOAD DATA LOCAL INPATH 'Reviews_cleaned.csv' OVERWRITE INTO TABLE apprezzamento_utenti;

-- Calcolo dell'apprezzamento medio per ogni utente
SELECT UserId, AVG(HelpfulnessNumerator / HelpfulnessDenominator) AS Apprezzamento
FROM apprezzamento_utenti
GROUP BY UserId
ORDER BY Apprezzamento DESC
LIMIT 50;

