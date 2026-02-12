import csv
from pathlib import Path

fp = Path(r"data/interim/spain_covid_MH_2020-03-01_2020-03-08.csv")
needle = "vinicius"  # cambia si quieres

with fp.open("r", encoding="utf-8", newline="") as f:
    r = csv.DictReader(f)
    for row in r:
        url = (row.get("url") or "").lower()
        title = (row.get("title") or "").lower()
        if needle in url or needle in title:
            print("TITLE:", row.get("title"))
            print("URL:", row.get("url"))
            print("mh_matches:", row.get("mh_matches"))
            txt = row.get("plain_text") or ""
            print("\nTEXT (first 600 chars):\n", txt[:600])
            break
    else:
        print("No match found for:", needle)
