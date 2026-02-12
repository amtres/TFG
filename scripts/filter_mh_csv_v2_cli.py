import argparse
import csv
import re
import unicodedata
from pathlib import Path

csv.field_size_limit(10_000_000)

DEFAULT_HARVEST_DIR = Path("data/raw/harvest")
DEFAULT_OUT_DIR = Path("data/interim/filters/mh_v2_strict_covidOK")
DEFAULT_MH_FILE = Path("data/metadata/keywords/keywords_mh_strict.txt")

DEFAULT_COVID_TERMS = [
    "covid", "covid-19", "coronavirus", "sars-cov-2", "pandemia",
    "confinamiento", "cuarentena", "estado de alarma", "desescalada", "toque de queda"
]

def norm(s: str) -> str:
    s = s.lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return s

def load_keywords(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Keyword file not found: {path}")
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
        # phrase vs single token
        if " " in k:
            pat = re.compile(re.escape(k))
        else:
            pat = re.compile(rf"\b{re.escape(k)}\b")
        pats.append((kw, pat))
    return pats

def month_from_filename(name: str) -> str | None:
    # expects: spain_covid_broad_YYYY-MM-DD_YYYY-MM-DD.csv
    m = re.search(r"spain_covid_broad_(\d{4}-\d{2})-\d{2}_\d{4}-\d{2}-\d{2}\.csv$", name)
    return m.group(1) if m else None

def year_from_filename(name: str) -> str | None:
    m = re.search(r"spain_covid_broad_(\d{4})-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.csv$", name)
    return m.group(1) if m else None

def filter_file(in_fp: Path, out_fp: Path, mh_pats, covid_pats, max_chars: int = 6000, dedup_url: bool = True):
    n_in = n_out = 0
    seen = set()

    with in_fp.open("r", encoding="utf-8-sig", newline="") as fin:
        reader = csv.DictReader(fin)
        fields = list(reader.fieldnames or [])
        for extra in ("mh_matches", "covid_matches"):
            if extra not in fields:
                fields.append(extra)

        out_fp.parent.mkdir(parents=True, exist_ok=True)
        with out_fp.open("w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()

            for row in reader:
                n_in += 1

                if dedup_url:
                    url = (row.get("url") or "").strip()
                    if url:
                        if url in seen:
                            continue
                        seen.add(url)

                title = row.get("title") or ""
                text = row.get("plain_text") or ""
                head = norm((title + "\n" + text)[:max_chars])

                covid_hits = [t for t, pat in covid_pats if pat.search(head)]
                if not covid_hits:
                    continue

                mh_hits = [kw for kw, pat in mh_pats if pat.search(head)]
                if not mh_hits:
                    continue

                row["covid_matches"] = ";".join(covid_hits)
                row["mh_matches"] = ";".join(mh_hits)
                writer.writerow(row)
                n_out += 1

    return n_in, n_out

def main():
    ap = argparse.ArgumentParser(description="Filter COVID harvest corpus into MH (strict) subcorpus with covid co-occurrence.")
    ap.add_argument("--harvest-dir", default=str(DEFAULT_HARVEST_DIR), help="Directory with spain_covid_broad_*.csv files")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory for filtered files")
    ap.add_argument("--mh-keywords", default=str(DEFAULT_MH_FILE), help="MH keywords file (one per line)")
    ap.add_argument("--covid-terms", nargs="*", default=DEFAULT_COVID_TERMS, help="COVID terms list")
    ap.add_argument("--month", help="Filter only one month, format YYYY-MM (e.g., 2020-04)")
    ap.add_argument("--year", help="Filter only one year, format YYYY (e.g., 2020)")
    ap.add_argument("--max-chars", type=int, default=6000)
    ap.add_argument("--no-dedup", action="store_true", help="Do not deduplicate by URL")
    args = ap.parse_args()

    harvest_dir = Path(args.harvest_dir)
    out_dir = Path(args.out_dir)
    mh_file = Path(args.mh_keywords)

    mh_pats = compile_patterns(load_keywords(mh_file))
    covid_pats = [(t, re.compile(rf"\b{re.escape(norm(t))}\b")) for t in args.covid_terms]

    files = sorted(harvest_dir.glob("spain_covid_broad_*.csv"))
    if not files:
        raise SystemExit(f"No files found in {harvest_dir} matching spain_covid_broad_*.csv")

    # filter selection
    selected = []
    for fp in files:
        if args.month:
            if month_from_filename(fp.name) == args.month:
                selected.append(fp)
        elif args.year:
            if year_from_filename(fp.name) == args.year:
                selected.append(fp)
        else:
            selected.append(fp)

    if not selected:
        raise SystemExit("No files matched your --month/--year selection.")

    grand_in = grand_out = 0
    for fp in selected:
        out_fp = out_dir / fp.name.replace("spain_covid_broad_", "spain_covid_MH_strict_covidOK_")
        n_in, n_out = filter_file(
            fp, out_fp, mh_pats, covid_pats,
            max_chars=args.max_chars,
            dedup_url=(not args.no_dedup)
        )
        grand_in += n_in
        grand_out += n_out
        pct = (100*n_out/n_in) if n_in else 0
        print(f"{fp.name}: in={n_in}  out={n_out}  ({pct:.2f}%) -> {out_fp}")

    total_pct = (100*grand_out/grand_in) if grand_in else 0
    print(f"\nTOTAL: in={grand_in}  out={grand_out}  ({total_pct:.2f}%)")

if __name__ == "__main__":
    main()
