import csv

input_file = "Reviews_augmented.csv"
output_file = "Reviews_processed.csv"

fields_to_keep = ["ProductId", "Time", "Score", "Text"]

with open(input_file, "r") as in_file, open(output_file, "w", newline="") as out_file:
    reader = csv.DictReader(in_file)
    writer = csv.DictWriter(out_file, fieldnames=fields_to_keep)
    writer.writeheader()
    for row in reader:
        new_row = {}
        for field in fields_to_keep:
            new_row[field] = row[field]
        writer.writerow(new_row)

