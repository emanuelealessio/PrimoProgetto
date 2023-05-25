#!/usr/bin/env python3

import sys
from collections import defaultdict
import heapq

# Definiamo un dizionario per tenere traccia del numero di recensioni per prodotto per anno
reviews_count = defaultdict(lambda: defaultdict(int))

# Definiamo un dizionario per tenere traccia del numero di occorrenze di parole per prodotto per anno
words_count = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

# Leggiamo le coppie chiave-valore emesse dal mapper
for line in sys.stdin:
    fields = line.strip().split('\t')
    year = fields[0]
    product_id = fields[1]

    # Se il valore è "1", incrementiamo il conteggio delle recensioni per il prodotto per l'anno corrispondente
    if fields[2] == "1":
        reviews_count[year][product_id] += 1
    # Altrimenti, incrementiamo il conteggio delle parole per il prodotto per l'anno corrispondente
    else:
        word = fields[2]
        count = int(fields[3])
        words_count[year][product_id][word] += count

# Per ogni anno, per ogni prodotto, determiniamo le 5 parole più frequenti con almeno 4 caratteri
# e le stampiamo insieme al numero di occorrenze
for year, products in reviews_count.items():
    for product_id, _ in heapq.nlargest(10, products.items(), key=lambda x: x[1]):
        words = words_count[year][product_id]
        top_words = heapq.nlargest(5, words.items(), key=lambda x: x[1])
        output = f"{year}\t{product_id}"
        for word, count in top_words:
            output += f"\t{word}:{count}"
        print(output)


