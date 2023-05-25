import csv
import random
import shutil

# Path del file originale
original_file_path = "Reviews.csv"

# Path del file di output
output_file_path = "Reviews_augmented.csv"

# Numero di copie da generare per ogni riga del dataset
num_copies = 2

# Copia il file originale in quello di output
shutil.copy(original_file_path, output_file_path)

# Apre il file originale in modalità lettura e il file di output in modalità scrittura
with open(original_file_path, mode="r") as original_file, open(output_file_path, mode="a", newline="") as output_file:
    # Legge il file CSV originale
    reader = csv.reader(original_file)
    # Scrive il file CSV di output
    writer = csv.writer(output_file)
    # Copia l'intestazione del file originale nel file di output
    header = next(reader)
    writer.writerow(header)
    # Copia ogni riga del file originale nel file di output, generando copie dei dati
    for row in reader:
        # Scrive la riga originale nel file di output
        writer.writerow(row)
        # Genera copie dei dati della riga e scrive ogni copia nel file di output
        for i in range(num_copies):
            new_row = list(row)
            new_row[0] = str(int(row[0]) + i + 1)
            new_row[4] = str(int(row[4]) + random.randint(1, 10))
            new_row[5] = str(int(row[5]) + random.randint(1, 10))
            new_row[6] = str(random.randint(1, 5))
            writer.writerow(new_row)

print("File di output generato correttamente!")

