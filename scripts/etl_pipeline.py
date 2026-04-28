from __future__ import annotations

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd



ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / "amazon_products_sales_data_uncleaned.csv"
PROCESSED_DIR = ROOT / "data" / "processed"
CLEAN_PATH = PROCESSED_DIR / "amazon_products_clean.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("etl")




_NUM_RE = re.compile(r"[-+]?\d*\.?\d+")
_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def _to_str(value) -> str:
    return "" if pd.isna(value) else str(value).strip()


def parse_currency(value) -> float:



    s = _to_str(value).replace(",", "")
    if not s:
        return np.nan
    m = _NUM_RE.search(s)
    return float(m.group()) if m else np.nan


def parse_count_shorthand(value) -> float:

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


    s = _to_str(value)
    if not s:
        return np.nan
    m = _NUM_RE.search(s)
    return float(m.group()) if m else np.nan


def parse_coupon_pct(value) -> float:
    s = _to_str(value).lower()
    if not s or "no coupon" in s:

        return 0.0
    m = _PCT_RE.search(s)

    return float(m.group(1)) if m else np.nan


def parse_bool_badge(value, true_token: str) -> bool:


    s = _to_str(value).lower()

    if not s:
        return False
    return true_token.lower() in s


def extract_brand(title: str) -> str:

    s = _to_str(title)
    if not s:
        return "Unknown"
    
    first = s.split()[0]

    first = re.sub(r"[^A-Za-z0-9&-]", "", first)

    if not first or first.isdigit() or len(first) < 2:

        return "Unknown"
    
    return first.title()


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



    if "is_couponed" in df.columns:
        df["coupon_pct"] = df["is_couponed"].apply(parse_coupon_pct)
        df["has_coupon"] = df["coupon_pct"].fillna(0).gt(0)


    if "sustainability_badges" in df.columns:
        df["has_sustainability_badge"] = df["sustainability_badges"].notna() & (
            df["sustainability_badges"].astype(str).str.strip() != ""
        )




    if "collected_at" in df.columns:
        df["collected_at"] = pd.to_datetime(df["collected_at"], errors="coerce")

    log.info("Coerced dtypes:\n%s", df.dtypes)
    return df


def drop_unusable_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    before = len(df)


    price_cols = [c for c in ("current_discounted_price", "listed_price") if c in df.columns]
    df = df.dropna(subset=["title"])
    if price_cols:
        df = df[df[price_cols].notna().any(axis=1)]
    df = df.drop_duplicates(subset=["title"])

    log.info("Dropped %s unusable / duplicate rows → %s remain", before - len(df), len(df))
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()


    if {"current_discounted_price", "listed_price"} <= set(df.columns):
        df["selling_price"] = df["current_discounted_price"].fillna(df["listed_price"])
    elif "listed_price" in df.columns:
        df["selling_price"] = df["listed_price"]


    if {"listed_price", "current_discounted_price"} <= set(df.columns):
        df["discount_pct"] = np.where(
            df["listed_price"].gt(0)
            & df["current_discounted_price"].notna()
            & (df["current_discounted_price"] < df["listed_price"]),
            (df["listed_price"] - df["current_discounted_price"]) / df["listed_price"] * 100,
            0.0,
        ).round(2)
        df["is_discounted"] = df["discount_pct"] > 0



    if "current_discounted_price" in df.columns:
        df["current_discounted_price"] = df["current_discounted_price"].fillna(df["selling_price"])
    if "listed_price" in df.columns:
        df["listed_price"] = df["listed_price"].fillna(df["selling_price"])



    before = len(df)
    if "rating" in df.columns:
        df = df.dropna(subset=["rating"]).reset_index(drop=True)
    log.info("Dropped %s rows missing rating", before - len(df))




    for col in ("number_of_reviews", "bought_in_last_month"):
        if col in df.columns:
            df[col] = df[col].fillna(0)



    if "coupon_pct" in df.columns:
        df["coupon_pct"] = df["coupon_pct"].fillna(0)


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



    remaining = df.isna().sum()
    remaining = remaining[remaining > 0]
    if len(remaining):
        log.warning("Columns still containing NaN:\n%s", remaining)
    else:
        log.info("No NaN values remain in cleaned dataset")

    log.info("Final columns: %s", list(df.columns))
    return df


def drop_noise_columns(df: pd.DataFrame) -> pd.DataFrame:

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
