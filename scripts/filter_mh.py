import json
import re
import unicodedata
from pathlib import Path

def norm(s: str) -> str:
    s = s.lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return s

def load_keywords(path: Path) -> list[str]:
    kws = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        kws.append(line)
    return kws

def compile_patterns(kws: list[str]) -> list[tuple[str, re.Pattern]]:
    out = []
    for kw in kws:
        k = norm(kw)
        if " " in k:
            pat = re.compile(re.escape(k))
        else:
            pat = re.compile(rf"\b{re.escape(k)}\b")
        out.append((kw, pat))
    return out

def get_text(rec: dict) -> str:
    parts = []
    for f in ("title", "description", "text", "content"):
        v = rec.get(f)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    return "\n".join(parts)

def filter_file(in_path: Path, out_path: Path, patterns, badlog_path: Path):
    n_in = 0
    n_out = 0
    n_bad = 0

    # utf-8-sig elimina BOM si existe
    with in_path.open("r", encoding="utf-8-sig", errors="replace") as fin, \
         out_path.open("w", encoding="utf-8") as fout, \
         badlog_path.open("a", encoding="utf-8") as blog:

        for lineno, raw in enumerate(fin, start=1):
            line = raw.strip()
            if not line:
                continue

            n_in += 1

            # Quita BOM residual si quedase
            line = line.lstrip("\ufeff")

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_bad += 1
                # guarda una muestra para inspección
                blog.write(f"{in_path.name}\tline={lineno}\t{line[:200]}\n")
                continue

            txt = norm(get_text(rec))
            hits = [kw for kw, pat in patterns if pat.search(txt)]
            if hits:
                rec["_mh_matches"] = hits
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n_out += 1

    return n_in, n_out, n_bad

def main():
    kw_path = Path("data/metadata/keywords_mh_strict.txt")
    in_dir = Path(r"data/raw")
    out_dir = Path(r"data/interim")
    out_dir.mkdir(parents=True, exist_ok=True)

    patterns = compile_patterns(load_keywords(kw_path))

    files = sorted(in_dir.glob("spain_covid_broad_2020-03-*.jsonl"))
    if not files:
        raise SystemExit("No input files found in data/raw (expected spain_covid_broad_2020-03-*.jsonl)")

    badlog = out_dir / "_bad_json_lines.log"
    # limpia log anterior
    if badlog.exists():
        badlog.unlink()

    total_in = 0
    total_out = 0
    total_bad = 0

    for fp in files:
        out_fp = out_dir / fp.name.replace("spain_covid_broad", "spain_covid_MH")
        n_in, n_out, n_bad = filter_file(fp, out_fp, patterns, badlog)
        total_in += n_in
        total_out += n_out
        total_bad += n_bad
        print(f"{fp.name}: in={n_in}  mh={n_out}  bad_lines={n_bad}  -> {out_fp.name}")

    print(f"\nTOTAL: in={total_in}  mh={total_out}  bad_lines={total_bad}  ({(total_out/total_in*100 if total_in else 0):.2f}%)")
    if total_bad:
        print(f"Bad lines log: {badlog}")

if __name__ == "__main__":
    main()
