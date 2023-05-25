import csv

# Lista dei campi da mantenere nel nuovo dataset
keep_fields = ["ProductId", "UserId", "HelpfulnessNumerator", "HelpfulnessDenominator", "Score"]

# Apri il file di input e crea il file di output
with open("Reviews_augmented.csv", "r", encoding="utf8") as input_file, open("Reviews_cleaned.csv", "w", newline="", encoding="utf8") as output_file:
    
    # Leggi il file di input come dizionario CSV
    reader = csv.DictReader(input_file)
    
    # Scrivi l'intestazione del file di output
    writer = csv.DictWriter(output_file, fieldnames=keep_fields)
    writer.writeheader()
    
    # Copia solo i campi necessari dal file di input al file di output
    for row in reader:
        cleaned_row = {field: row[field] for field in keep_fields}
        writer.writerow(cleaned_row)

