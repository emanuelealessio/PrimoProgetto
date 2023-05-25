#!/usr/bin/env python3
import sys
import csv
from collections import defaultdict
import heapq

# Definiamo una funzione per rimuovere la punteggiatura e le parole vuote dal testo
def clean_text(text):
    punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    for char in punctuation:
        text = text.replace(char, ' ')
    return ' '.join(text.split())

# Definiamo la funzione per estrarre le parole dal testo e contarle
def extract_words_count(text):
    words_count = defaultdict(int)
    for word in text.split():
        if len(word) >= 4:
            words_count[word] += 1
    return words_count

# Leggiamo il file CSV di input e processiamo le recensioni
for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) == 4:
        product_id = fields[0]
        time = fields[1]
        score = fields[2]
        text = fields[3]
        year = time[:4]
        # Emittiamo coppie chiave-valore per conteggio delle recensioni per prodotto per anno
        print(f"{year}\t{product_id}\t1")
        # Emittiamo coppie chiave-valore per conteggio delle parole per prodotto per anno
        cleaned_text = clean_text(text)
        words_count = extract_words_count(cleaned_text)
        for word, count in words_count.items():
            print(f"{year}\t{product_id}\t{word}\t{count}")


