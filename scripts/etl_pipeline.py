"""
ETL Pipeline — Amazon Products Sales Dataset (Capstone 2, E-commerce sector).

Source columns (uncleaned variant):
    title, rating, number_of_reviews, bought_in_last_month,
    current/discounted_price, price_on_variant, listed_price,
    is_best_seller, is_sponsored, is_couponed, buy_box_availability,
    delivery_details, sustainability_badges, image_url, product_url,
    collected_at

Run from project root:
    python scripts/etl_pipeline.py
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths & logging
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / "amazon_products_sales_data_uncleaned.csv"
PROCESSED_DIR = ROOT / "data" / "processed"
CLEAN_PATH = PROCESSED_DIR / "amazon_products_clean.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("etl")


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
_NUM_RE = re.compile(r"[-+]?\d*\.?\d+")
_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def _to_str(value) -> str:
    return "" if pd.isna(value) else str(value).strip()


def parse_currency(value) -> float:
    """'$159.00' / '89.68' / '₹1,299.00' → 159.00 / 89.68 / 1299.00."""
    s = _to_str(value).replace(",", "")
    if not s:
        return np.nan
    m = _NUM_RE.search(s)
    return float(m.group()) if m else np.nan


def parse_count_shorthand(value) -> float:
    """'2,457' / '6K+' / '1.2M' / '300+ bought in past month' → 2457 / 6000 / 1_200_000 / 300."""
    s = _to_str(value)
    if not s:
        return np.nan
    s = s.replace(",", "")
    mult = 1
    if re.search(r"\bM\b|\dM", s):
        mult = 1_000_000
    elif re.search(r"\bK\b|\dK", s):
        mult = 1_000
    m = _NUM_RE.search(s)
    return float(m.group()) * mult if m else np.nan


def parse_rating(value) -> float:
    """'4.6 out of 5 stars' → 4.6."""
    s = _to_str(value)
    if not s:
        return np.nan
    m = _NUM_RE.search(s)
    return float(m.group()) if m else np.nan


def parse_coupon_pct(value) -> float:
    """'Save 15% with coupon' → 15.0; 'No Coupon' → 0.0; '$5 off coupon' → np.nan (we only count %)."""
    s = _to_str(value).lower()
    if not s or "no coupon" in s:
        return 0.0
    m = _PCT_RE.search(s)
    return float(m.group(1)) if m else np.nan


def parse_bool_badge(value, true_token: str) -> bool:
    """Generic 'is something' parser — true if `true_token` appears in the cell, else False."""
    s = _to_str(value).lower()
    if not s:
        return False
    return true_token.lower() in s


def extract_brand(title: str) -> str:
    """Heuristic: brand is typically the first 1-3 tokens of the Amazon title.
    We take the first token, with light cleaning. Far from perfect — flagged in
    data dictionary as a heuristic feature."""
    s = _to_str(title)
    if not s:
        return "Unknown"
    first = s.split()[0]
    first = re.sub(r"[^A-Za-z0-9&-]", "", first)
    if not first or first.isdigit() or len(first) < 2:
        return "Unknown"
    return first.title()


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------
def load_raw(path: Path = RAW_PATH) -> pd.DataFrame:
    log.info("Loading raw dataset: %s", path)
    if not path.exists():
        raise FileNotFoundError(
            f"Raw dataset missing at {path}. See data/raw/README.md for download instructions."
        )
    df = pd.read_csv(path, low_memory=False)
    log.info("Raw shape: %s rows × %s cols", *df.shape)
    return df


def standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )
    log.info("Columns after standardisation: %s", list(df.columns))
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Numeric parses
    if "rating" in df.columns:
        df["rating"] = df["rating"].apply(parse_rating).clip(0, 5)
    if "number_of_reviews" in df.columns:
        df["number_of_reviews"] = df["number_of_reviews"].apply(parse_count_shorthand)
    if "bought_in_last_month" in df.columns:
        df["bought_in_last_month"] = df["bought_in_last_month"].apply(parse_count_shorthand)
    if "current_discounted_price" in df.columns:
        df["current_discounted_price"] = df["current_discounted_price"].apply(parse_currency)
    if "listed_price" in df.columns:
        df["listed_price"] = df["listed_price"].apply(parse_currency)

    # Boolean flags from string badges
    if "is_best_seller" in df.columns:
        df["is_best_seller"] = df["is_best_seller"].apply(
            lambda v: parse_bool_badge(v, "best") or parse_bool_badge(v, "amazon's choice")
        )
    if "is_sponsored" in df.columns:
        df["is_sponsored"] = df["is_sponsored"].apply(lambda v: parse_bool_badge(v, "sponsored"))
    if "buy_box_availability" in df.columns:
        df["has_buy_box"] = df["buy_box_availability"].apply(
            lambda v: parse_bool_badge(v, "add to cart") or parse_bool_badge(v, "buy now")
        )

    # Coupon % (0 if "No Coupon", NaN if non-percent dollar coupon)
    if "is_couponed" in df.columns:
        df["coupon_pct"] = df["is_couponed"].apply(parse_coupon_pct)
        df["has_coupon"] = df["coupon_pct"].fillna(0).gt(0)

    # Sustainability flag
    if "sustainability_badges" in df.columns:
        df["has_sustainability_badge"] = df["sustainability_badges"].notna() & (
            df["sustainability_badges"].astype(str).str.strip() != ""
        )

    # Datetime
    if "collected_at" in df.columns:
        df["collected_at"] = pd.to_datetime(df["collected_at"], errors="coerce")

    log.info("Coerced dtypes:\n%s", df.dtypes)
    return df


def drop_unusable_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    before = len(df)

    # A row needs at least a title and one of (current price, listed price)
    price_cols = [c for c in ("current_discounted_price", "listed_price") if c in df.columns]
    df = df.dropna(subset=["title"])
    if price_cols:
        df = df[df[price_cols].notna().any(axis=1)]
    df = df.drop_duplicates(subset=["title"])

    log.info("Dropped %s unusable / duplicate rows → %s remain", before - len(df), len(df))
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ---- Imputation policy (documented in docs/data_dictionary.md) ----
    # 1. Effective selling price = current_discounted_price if present, else listed_price
    if {"current_discounted_price", "listed_price"} <= set(df.columns):
        df["selling_price"] = df["current_discounted_price"].fillna(df["listed_price"])
    elif "listed_price" in df.columns:
        df["selling_price"] = df["listed_price"]

    # 2. Discount % BEFORE filling raw price columns (so we know which were missing)
    if {"listed_price", "current_discounted_price"} <= set(df.columns):
        df["discount_pct"] = np.where(
            df["listed_price"].gt(0)
            & df["current_discounted_price"].notna()
            & (df["current_discounted_price"] < df["listed_price"]),
            (df["listed_price"] - df["current_discounted_price"]) / df["listed_price"] * 100,
            0.0,
        ).round(2)
        df["is_discounted"] = df["discount_pct"] > 0

    # 3. Now fill the raw price columns symmetrically with selling_price (no-discount assumption)
    if "current_discounted_price" in df.columns:
        df["current_discounted_price"] = df["current_discounted_price"].fillna(df["selling_price"])
    if "listed_price" in df.columns:
        df["listed_price"] = df["listed_price"].fillna(df["selling_price"])

    # 4. Drop SKUs with no rating (~0.7% after dedupe). Rating is a key analytical field
    #    and median-imputing 4.5 would distort downstream tests.
    before = len(df)
    if "rating" in df.columns:
        df = df.dropna(subset=["rating"]).reset_index(drop=True)
    log.info("Dropped %s rows missing rating", before - len(df))

    # 5. Fill engagement counts with 0 (absence ≡ no recent activity / no reviews collected)
    for col in ("number_of_reviews", "bought_in_last_month"):
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # 6. Coupon % — NaN means $-coupon (not %). Coerce to 0; the boolean flag (has_coupon)
    #    still carries the correct presence signal.
    if "coupon_pct" in df.columns:
        df["coupon_pct"] = df["coupon_pct"].fillna(0)

    # ---- Engineered categorical / log features ----
    df["brand"] = df["title"].apply(extract_brand)

    if "selling_price" in df.columns:
        df["price_tier"] = pd.cut(
            df["selling_price"],
            bins=[-np.inf, 15, 50, 150, np.inf],
            labels=["Budget", "Mid", "Premium", "Luxury"],
        )

    if "rating" in df.columns:
        df["rating_bucket"] = pd.cut(
            df["rating"],
            bins=[-np.inf, 2.9, 3.9, 4.4, np.inf],
            labels=["Poor", "Average", "Good", "Excellent"],
        )

    if "number_of_reviews" in df.columns:
        df["log_reviews"] = np.log1p(df["number_of_reviews"])
    if "bought_in_last_month" in df.columns:
        df["log_bought_last_month"] = np.log1p(df["bought_in_last_month"])

    # ---- Final NaN sanity check ----
    remaining = df.isna().sum()
    remaining = remaining[remaining > 0]
    if len(remaining):
        log.warning("Columns still containing NaN:\n%s", remaining)
    else:
        log.info("✅ No NaN values remain in cleaned dataset")

    log.info("Final columns: %s", list(df.columns))
    return df


def drop_noise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop high-cardinality / non-analytical columns before writing."""
    drop = [c for c in ("image_url", "product_url", "price_on_variant",
                        "delivery_details", "is_couponed", "buy_box_availability",
                        "sustainability_badges")
            if c in df.columns]
    return df.drop(columns=drop)


def write_processed(df: pd.DataFrame, path: Path = CLEAN_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log.info("Wrote cleaned dataset: %s (%s rows × %s cols)", path, *df.shape)
    return path


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run() -> pd.DataFrame:
    df = load_raw()
    df = standardise_columns(df)
    df = coerce_types(df)
    df = drop_unusable_rows(df)
    df = engineer_features(df)
    df = drop_noise_columns(df)
    write_processed(df)
    return df


if __name__ == "__main__":
    run()
