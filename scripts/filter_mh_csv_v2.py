import csv
import re
import unicodedata
from pathlib import Path

csv.field_size_limit(10_000_000)

def norm(s: str) -> str:
    s = s.lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return s

def load_keywords(path: Path):
    kws = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        kws.append(line)
    return kws

def compile_patterns(kws):
    pats = []
    for kw in kws:
        k = norm(kw)
        if " " in k:
            pat = re.compile(re.escape(k))
        else:
            pat = re.compile(rf"\b{re.escape(k)}\b")
        pats.append((kw, pat))
    return pats

IN_FILES = sorted(Path("data/raw").glob("spain_covid_broad_2020-03-*.jsonl"))
OUT_DIR = Path("data/interim")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MH_FILE = Path("data/metadata/keywords_mh_strict.txt")
MH_PATS = compile_patterns(load_keywords(MH_FILE))

COVID_TERMS = [
    "covid", "covid-19", "coronavirus", "sars-cov-2", "pandemia",
    "confinamiento", "cuarentena", "estado de alarma"
]
COVID_PATS = [(t, re.compile(rf"\b{re.escape(norm(t))}\b")) for t in COVID_TERMS]

MAX_CHARS = 6000

total_in = total_out = 0

for fp in IN_FILES:
    out_fp = OUT_DIR / fp.name.replace("spain_covid_broad", "spain_covid_MH_strict_covidOK").replace(".jsonl", ".csv")
    n_in = n_out = 0
    seen_urls = set()

    with fp.open("r", encoding="utf-8-sig", newline="") as fin:
        reader = csv.DictReader(fin)
        fields = list(reader.fieldnames or [])
        for extra in ("mh_matches", "covid_matches"):
            if extra not in fields:
                fields.append(extra)

        with out_fp.open("w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()

            for row in reader:
                n_in += 1
                url = (row.get("url") or "").strip()
                if url:
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                title = row.get("title") or ""
                text = row.get("plain_text") or ""
                head = norm((title + "\n" + text)[:MAX_CHARS])

                covid_hits = [t for t, pat in COVID_PATS if pat.search(head)]
                if not covid_hits:
                    continue

                mh_hits = [kw for kw, pat in MH_PATS if pat.search(head)]
                if not mh_hits:
                    continue

                row["covid_matches"] = ";".join(covid_hits)
                row["mh_matches"] = ";".join(mh_hits)
                writer.writerow(row)
                n_out += 1

    total_in += n_in
    total_out += n_out
    print(f"{fp.name}: in={n_in}  mh_strict+covid={n_out}  -> {out_fp.name}")

print(f"\nTOTAL: in={total_in}  mh_strict+covid={total_out}  ({(100*total_out/total_in) if total_in else 0:.2f}%)")
