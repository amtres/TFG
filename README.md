# Semantic Framing of Mental Health During COVID-19

## A Subspace Modeling Approach to Media Representation Analysis

> **Final Degree Project (TFG) — Bachelor in Data and Business Analytics**
>
> IE University · School of Science & Technology
>
> **Author**: Álvaro Mengotti Medina · **Supervisor**: Prof. Alejandro Martínez-Mingo
>
> April 2026

---

## About This Project

This repository contains the computational pipeline and research artifacts for a study on **how Spanish newspapers framed mental health during the COVID-19 pandemic**, and how that framing evolved over time.

> **Research Question**: How did Spanish news media frame mental health during the COVID-19 pandemic, and how did the meanings, emotions, and narrative relevance of key mental health concepts evolve over time?

The system combines **Transformer-based NLP** (Domain-Adaptive Pretraining + contextual embeddings) with **Semantic Subspace Modeling** to quantitatively track how concepts like *anxiety*, *depression*, *isolation*, or *therapy* shift in meaning across time windows in the press.

### Key Results

The analysis revealed a clear **three-phase framing trajectory**:

| Phase | Period | Dominant Frame | Characteristics |
|---|---|---|---|
| **Shock** | Spring 2020 | Affective | High entropy, emotional trauma (fear, panic) |
| **Social Peak** | Autumn 2020 | Social | Second Wave, community friction, return to school/work |
| **Institutionalization** | Late 2020 – Early 2021 | Functional | Chronic crisis management, vaccine discourse |

### Research Specifications

| Aspect | Details |
|---|---|
| **Research target** | COVID-19 × Mental Health media framing |
| **Country & press** | Spain (El País, El Mundo, La Vanguardia, El Diario, El Confidencial, etc.) |
| **Keywords** | `salud mental`, `ansiedad`, `depresión`, `suicidio`, `estrés`, `terapia`, `miedo`, `soledad`, etc. |
| **Anchor dimensions** | **Functional** (clinical services, policy), **Social** (isolation, community), **Affective** (anxiety, fear, trauma) |
| **Time span** | March 2020 – March 2021 |
| **Base model** | BETO (`dccuchile/bert-base-spanish-wwm-uncased`) + DAPT |
| **Corpus** | 53,055 COVID articles → 2,156 MH-filtered → 7,535 keyword occurrences |

---

## How It Works

The system is orchestrated through a single master CLI: `pipeline_manager.py`.

### Phase 1 — Data Harvesting

*Resilient news collection infrastructure.*

- **"Day × Media" strategy**: Iterates **day by day** and **outlet by outlet**, bypassing GDELT's return cap (max 250 records) and ensuring near-complete historical coverage.
- **Source**: GDELT (Global Database of Events, Language, and Tone).
- **Resilience**:
  - Handles "Soft 404s" and JS-rendered content through domain-specific CSS selectors.
  - Automatic fallback to `trafilatura` for clean text extraction.
- **Two-stage filtering**: Broad COVID-19 filter → Strict mental health keyword filter.

### Phase 2 — NLP Infrastructure

*Transforming text into calibrated mathematical representations.*

#### 2.1 Model Management
The system supports any Hugging Face model:
- **BETO** (`dccuchile/bert-base-spanish-wwm-uncased`): 12 transformer layers, d=768, Whole Word Masking. 

#### 2.2 DAPT (Domain-Adaptive Pretraining)
Before extracting embeddings, the base model is further pretrained on the collected corpus.
- **Why**: A generic model may not capture domain-specific semantics. For instance, "ola" meaning a wave of infections rather than an ocean wave, or "confinamiento" carrying psychological connotations specific to the pandemic context.
- **Training**: 1 epoch on MH-strict corpus (15.4 MB), ~8h 49m.

#### 2.3 Contextual Embedding Extraction
For each mention of a target keyword (e.g., "salud mental"):
1. **Tokenization**: The keyword is located in the sentence. If fragmented into sub-tokens, **Mean Pooling** produces a single vector.
2. **Layer Strategy**:
   - **`penultimate`** (d=768): Second-to-last layer — optimal for general semantic representations. **Selected for this study.**
   - **`last4_concat`** (d=3,072): Concatenation of the last 4 layers — captures syntactic nuance but introduces structural noise.

### Phase 3 — Subspace Analysis

*Where the sociological insights emerge.*

#### 3.1 Anisotropy Correction
Language models exhibit **anisotropy**: vectors cluster in a narrow cone, distorting cosine distances. The pipeline implements:
1. **RAW**: Embeddings as-is from the model.
2. **CORRECTED**: Global mean vector subtracted from each embedding. This centers the point cloud, revealing the true semantic structure.

**Key finding**: Subspace projections are mathematically **invariant** under anisotropy correction, while centroid-based methods are contaminated by geometric noise.

#### 3.2 Dynamic Subspaces
Embeddings are grouped into **3-month rolling windows** (1-month step) and **SVD** is applied to extract principal axes of meaning in each period. This yields 11 temporal subspaces.

#### 3.3 Framing Metrics
- **Semantic Drift**: Grassmannian distance between consecutive subspaces. Measures the rate of structural meaning change.
- **Shannon Entropy**: Dispersion of singular values. High entropy = ambiguous, polysemous discourse.
- **Intrinsic Dimensionality**: Number of meaningful principal components (via Horn's Parallel Analysis).
- **Anchor Projection**: The target subspace is projected onto three theoretically predefined axes to quantify framing orientation:

| Dimension | Description | Example anchors |
|---|---|---|
| **Functional** | Clinical services, institutional response, policy | *psicólogo, terapia, ERTE, teletrabajo, tratamiento* |
| **Social** | Relationships, isolation, community | *aislamiento, familia, violencia, duelo, solidaridad* |
| **Affective** | Emotional states, psychological symptoms | *ansiedad, depresión, miedo, trauma, suicidio* |

### Phase 4 — Automated Reports
Generates Jupyter Notebooks and visualizations (heatmaps, time series, trajectory plots) for analysis and interpretation.

---

## Execution Guide

The script `pipeline_manager.py` is the single entry point.

### 0. Initial Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Verify the media list
cat data/metadata/media_lists/media_list.csv
```

### 1. Download Models

```bash
python pipeline_manager.py phase2 download-models \
    --models "dccuchile/bert-base-spanish-wwm-uncased"
```

### 2. Phase 1: Harvesting

```bash
python pipeline_manager.py phase1 \
    --keyword "covid" "coronavirus" "pandemia" \
    --from 2020-03-01 --to 2021-03-31 \
    --country SP \
    --media-list data/metadata/media_lists/media_list.csv \
    --output data/raw/spain_covid_broad.csv
```

### 3. Phase 2: NLP Processing

#### Step 3.1: DAPT (Recommended)

```bash
python pipeline_manager.py phase2 dapt \
    --data data/interim/datasets/spain_covidMHstrict_2020-03_2021-03_ALL.txt \
    --output models/beto_dapt_spain_MHstrict \
    --model "dccuchile/bert-base-spanish-wwm-uncased" \
    --epochs 1
```

#### Step 3.2: Embedding Extraction

```bash
python pipeline_manager.py phase2 extract \
    --data_dir data/raw \
    --output data/interim/embeddings/embeddings_covid_mh.parquet \
    --model "dccuchile/bert-base-spanish-wwm-uncased" \
    --dapt_model models/beto_dapt_spain_MHstrict \
    --keywords "salud mental" "ansiedad" "depresion" "estres" "suicidio" \
               "soledad" "miedo" "psicosis" "psicologo" "terapia" \
               "autolesion" "trastorno mental" "psiquiatria"
```

### 4. Phase 3: Subspace Analysis

```bash
python pipeline_manager.py phase3 \
    --input data/interim/embeddings/embeddings_covid_mh.parquet \
    --output-dir results/phase3_covid_mh \
    --baseline-model "dccuchile/bert-base-spanish-wwm-uncased" \
    --dapt-model models/beto_dapt_spain_MHstrict \
    --anchors data/metadata/anchors/dimensiones_ancla_mh_es_covid_FSA.json \
    --window-months 3
```

### 5. Phase 4: Report Generation

```bash
python pipeline_manager.py phase4 \
    --input results/phase3_covid_mh/phase3_results.csv \
    --output_dir results/final_report
```

---

## 📂 Repository Structure

```
TFG/
├── academic/                       # Academic documentation
│   ├── INTRO_TFG.md                # Research introduction and theoretical framework
│   └── methodological_report/      # Methodological reports (6 sections + EDA notebooks)
├── configs/                        # Pipeline configuration files
│   └── tfg_phase1_plan.yml         # Phase 1 harvesting plan
├── data/                           # Data (Gitignored except metadata)
│   ├── metadata/
│   │   ├── anchors/                # Anchor dimension definitions (JSON)
│   │   ├── keywords/               # Keyword lists (COVID, mental health)
│   │   └── media_lists/            # Spanish media outlet catalogues
│   ├── interim/
│   │   ├── datasets/               # Filtered corpora
│   │   └── embeddings/             # Extracted embeddings
│   └── raw/                        # Raw harvested news articles
├── models/                         # Trained/adapted models (Gitignored)
├── notebooks/                      # Interactive analysis notebooks
│   ├── Harvest_Report_TFG.ipynb    # Data collection summary & stats
│   └── phase3_analysis_results.ipynb
├── results/                        # Phase 3 & 4 outputs
├── runs/                           # Execution logs and progress tracking
├── scripts/                        # Utility scripts
├── src/                            # Source Code
│   ├── news_harvester/             # Scraping logic (domains, selectors)
│   ├── nlp/                        # DAPT, embedding extraction, anchors
│   ├── subspace_analysis/          # SVD, Grassmannian distance, entropy
│   ├── reporting/                  # Report generation logic
│   └── visualization/              # Plotting and visualization utilities
├── pipeline_manager.py             # Master CLI entry point
├── requirements.txt                # Python dependencies
├── Final_thesis_v1.docx            # Written thesis document
└── README.md                       # This file
```

---

## Data Storage & Backups

- All project files are version-controlled **except large datasets** (>100 MB).
- Large harvesting outputs are ignored via `.gitignore`.

---

## Acknowledgments

This project builds upon the [**LISBETH**](https://github.com/alejandrommingo/LISBETH) computational framework, developed by Prof. Alejandro Martínez-Mingo for the analysis of semantic evolution in media discourse. The pipeline architecture, subspace analysis methodology, and framing metrics were adapted from this framework and applied to the study of COVID-19 and mental health media framing in Spanish press.

I would like to thank my colleagues — Jaime Calventi, Hassan Chapra, and Eduardo Moreno — for their support and for the valuable insights they developed throughout their respective projects.

Although we worked on different topics, all within the broader framework of Semantic Framing and subspaces, our shared discussions and parallel progress significantly informed and enriched this work.

--
## Contact

If you have any questions or suggestions, please feel free to contact me at amedina.ieu2022@student.ie.edu

---
**Semantic Framing of Mental Health During COVID-19 - 10  April 2026**