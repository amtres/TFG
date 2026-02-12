import csv
import re
import unicodedata
from pathlib import Path

# Evita errores si algún artículo es muy largo
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

kw_path = Path("data/metadata/keywords_mh_strict.txt")
patterns = compile_patterns(load_keywords(kw_path))

in_files = sorted(Path("data/raw").glob("spain_covid_broad_2020-03-*.jsonl"))
out_dir = Path("data/interim")
out_dir.mkdir(parents=True, exist_ok=True)

total_in = total_out = 0

for fp in in_files:
    out_fp = out_dir / fp.name.replace("spain_covid_broad", "spain_covid_MH").replace(".jsonl", ".csv")
    n_in = n_out = 0
    seen_urls = set()

    with fp.open("r", encoding="utf-8-sig", newline="") as fin:
        reader = csv.DictReader(fin)
        fields = list(reader.fieldnames or [])
        if "mh_matches" not in fields:
            fields.append("mh_matches")

        with out_fp.open("w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()

            for row in reader:
                n_in += 1

                # dedup por URL si existe
                url = (row.get("url") or "").strip()
                if url:
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                # concatena todo el row para ser robustos
                txt = norm(" | ".join(v for v in row.values() if v))

                hits = [kw for kw, pat in patterns if pat.search(txt)]
                if not hits:
                    continue

                row["mh_matches"] = ";".join(hits)
                writer.writerow(row)
                n_out += 1

    total_in += n_in
    total_out += n_out
    print(f"{fp.name}: in={n_in}  mh={n_out}  -> {out_fp.name}")

print(f"\nTOTAL: in={total_in}  mh={total_out}  ({(100*total_out/total_in) if total_in else 0:.2f}%)")
