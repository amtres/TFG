import csv
from pathlib import Path

files = sorted(Path("data/raw").glob("spain_covid_broad_2020-03-*.jsonl"))
total = 0

for fp in files:
    with fp.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)  # skip header
        n = sum(1 for _ in reader)
    print(f"{fp.name}: {n}")
    total += n

print(f"\nTOTAL: {total}")
