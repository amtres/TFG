import pandas as pd
import glob
import os
import argparse

def merge_many(in_glob: str, out_file: str, dedup_col: str = "url"):
    files = sorted(glob.glob(in_glob))
    if not files:
        raise SystemExit(f"No files matched: {in_glob}")

    print(f"Found {len(files)} files.")
    dfs = []
    for fp in files:
        try:
            df = pd.read_csv(fp, encoding="utf-8-sig")
            dfs.append(df)
            print(f"  + {os.path.basename(fp)}  rows={len(df)}")
        except Exception as e:
            print(f"  ! Skipping {fp}: {e}")

    if not dfs:
        raise SystemExit("No readable CSV files.")

    combined = pd.concat(dfs, ignore_index=True)

    # Dedup
    if dedup_col in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(subset=[dedup_col])
        after = len(combined)
        print(f"Dedup by '{dedup_col}': {before} -> {after}  (removed {before-after})")
    else:
        print(f"WARNING: '{dedup_col}' not in columns. No dedup applied.")

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    combined.to_csv(out_file, index=False, encoding="utf-8")
    print(f"OK -> {out_file}  rows={len(combined)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_glob", required=True, help=r'Glob, e.g. "data/raw/harvest/spain_covid_broad_2020-*.csv"')
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--dedup_col", default="url", help="Column to deduplicate by (default: url)")
    args = ap.parse_args()

    merge_many(args.in_glob, args.out, args.dedup_col)

if __name__ == "__main__":
    main()
