import json
import re
import unicodedata
from pathlib import Path

INP = Path(r"data/metadata/anchors/dimensiones_ancla_mh_es_covid_FSA.json")
OUT = Path(r"data/metadata/anchors/dimensiones_ancla_mh_es_covid_FSA_ascii.json")

def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def clean(s: str) -> str:
    # elimina tildes y normaliza espacios
    s = strip_accents(s)
    s = s.replace("’", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"\s+", " ", s).strip()
    return s

data = json.loads(INP.read_text(encoding="utf-8"))

fixed = 0
for dim, obj in data.items():
    for a in obj.get("anchors", []):
        kw = clean(a["keyword"].lower())
        sent = clean(a["sentence"])
        # fuerza que el keyword aparezca
        if kw.lower() not in sent.lower():
            # lo añadimos de forma sencilla al principio
            sent = f"{kw} - {sent}"
        a["keyword"] = kw
        a["sentence"] = sent
        fixed += 1

OUT.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"OK: wrote {OUT} ({fixed} anchors fixed)")
