# DVA Capstone 2 — Amazon Products Sales Analytics

**Sector:** E-commerce / Retail
**Institute:** Newton School of Technology
**Tools:** Python (Jupyter), Tableau Public, GitHub

## Problem Statement (Draft)
Analyse a raw Amazon products catalogue (~42K SKUs, 2025) to identify the **pricing, rating, discount, and category patterns that drive review volume and visibility**, and translate those findings into actionable portfolio decisions for an Amazon seller / category manager.

> Final problem statement to be locked in after Gate 1 mentor approval.

## Dataset
- **Source:** [Kaggle — Amazon Products Sales Dataset (42K, 2025) — uncleaned](https://www.kaggle.com/datasets/ikramshah512/amazon-products-sales-dataset-42k-items-2025?select=amazon_products_sales_data_uncleaned.csv)
- **File:** `amazon_products_sales_data_uncleaned.csv` (36.7 MB, 42,675 × 16)
- **Cleaned output:** `data/processed/amazon_products_clean.csv` (5,971 × 21, zero NaN — produced by `scripts/etl_pipeline.py`)
- **Why this dataset (compliance with Capstone 2 input rules):**
  - **Raw / minimally processed** — prices stored as `"$159.00"`, ratings as `"4.6 out of 5 stars"`, review counts as `"6K+"`, coupons as `"Save 15% with coupon"`
  - **Row-level records** — 42,675 SKU-level rows
  - **Missing values & inconsistencies present** — 27.5% NaN in `current_discounted_price`, 54% NaN in `listed_price`, 92% NaN in `sustainability_badges`, 24,912 duplicate listings across pages
  - **No pre-built dashboard or feature-engineered version** — no `brand`, `price_tier`, `discount_pct`, or log-transforms in the source; all engineered columns are produced by our pipeline
  - **Volume rule** — 42K raw rows ≥ 5K minimum; 21 analytical columns ≥ 8 minimum

Place the downloaded CSV at: `data/raw/amazon_products_sales_data_uncleaned.csv` (never edit it).

## Repository Structure
```
DVA-capstone-2/
├── README.md
├── requirements.txt
├── data/
│   ├── raw/         # amazon_products_sales_data_uncleaned.csv (42,675 × 16, never edited)
│   └── processed/   # amazon_products_clean.csv (5,971 × 21, zero NaN)
├── notebooks/
│   ├── 01_extraction.ipynb
│   ├── 02_cleaning.ipynb
│   ├── 03_eda.ipynb
│   ├── 04_statistical_analysis.ipynb
│   └── 05_final_load_prep.ipynb
├── scripts/
│   └── etl_pipeline.py     # 42,675 → 5,971 cleaning + feature engineering
├── tableau/
│   ├── screenshots/        # filled at dashboard publication time
│   └── dashboard_links.md
├── reports/
│   ├── project_report.pdf
│   └── presentation.pdf
└── docs/
    └── data_dictionary.md
```

## Quickstart
```bash
# 1. Create & activate venv
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Drop the raw CSV into data/raw/
#    (download from the Kaggle link above)

# 4. Run the ETL pipeline (regenerates data/processed/amazon_products_clean.csv)
python scripts/etl_pipeline.py

# 5. Open the notebooks for the cell-by-cell narrative (01 → 02 → 03 → 04 → 05)
jupyter lab
```

## Workflow
1. **01_extraction** — load raw, profile schema, log issues
2. **02_cleaning** — handle nulls, types, outliers, dedupe → `data/processed/amazon_products_clean.csv` (5,971 × 21, zero NaN)
3. **03_eda** — trends, distributions, comparisons across brand/price-tier/rating-bucket
4. **04_statistical_analysis** — correlation, ANOVA, Welch's t-test, OLS regression, KMeans
5. **05_final_load_prep** — KPI computation; Tableau reads `amazon_products_clean.csv` directly (no separate extract)

## Team
| Role | Member | Primary Responsibility |
|---|---|---|
| Lead / Data Sourcing | Raman Pandey, Aashish Jha | Dataset selection, Gate 1 proposal |
| ETL Engineer | Raman Pandey, Manthan Ziman | Cleaning pipeline, scripts |
| Analyst — EDA | Uzair Ahmed Shah | Notebooks 03 |
| Analyst — Stats | Jagadish Ishwar Patil | Notebook 04 |
| Dashboard | Manthan Ziman, Uzair Ahmed Shah | Tableau |
| Reporting | Jagadish Ishwar Patil, Aashish Jha | Final report |

## Links
- **GitHub:** _add public repo URL here_
- **Tableau Public Dashboard:** _add after publishing_

## License
Educational use — Newton School of Technology Capstone 2.
