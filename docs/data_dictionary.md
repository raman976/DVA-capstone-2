# Data Dictionary — Amazon Products Sales Dataset (2025)

**Source:** Kaggle — *Amazon Products Sales Dataset (42K Items, 2025)* by `ikramshah512`
https://www.kaggle.com/datasets/ikramshah512/amazon-products-sales-dataset-42k-items-2025

**Raw file:** `amazon_products_sales_data_uncleaned.csv` (~36.7 MB)
**Raw shape:** 42,675 rows × 16 cols
**Cleaned shape:** 5,971 rows × 21 cols (after dedupe-by-title + price-required filter + drop SKUs with no rating). Zero NaN values in every analytical column.
**Currency:** USD (listed price uses `$`)
**Snapshot date:** ~2025-08-21 (`collected_at` column)

## Raw Schema (16 columns)
| Column | Raw Type | Description | Sample / Cleaning Need |
|---|---|---|---|
| `title` | string | Full product title (free text) | Long, contains brand + variant + spec |
| `rating` | string | Star rating string | `"4.6 out of 5 stars"` → extract float |
| `number_of_reviews` | string | Total review count | `"2,457"` → strip comma → int |
| `bought_in_last_month` | string | Recent purchases | `"6K+ bought in past month"` → 6000 |
| `current/discounted_price` | string | Selling price (numeric in $) | `"89.68"`; **27.5% missing** |
| `price_on_variant` | string | Per-variant price label | `"basic variant price: nan"` — mostly noise, **dropped** |
| `listed_price` | string | List/MRP price | `"$159.00"` → strip `$` → float |
| `is_best_seller` | string | Badge text | `"Best Seller"` / `"Amazon's Choice"` / `"No Badge"` → bool |
| `is_sponsored` | string | Ad slot flag | `"Sponsored"` / other → bool |
| `is_couponed` | string | Coupon offer text | `"Save 15% with coupon"` / `"No Coupon"` → coupon_pct |
| `buy_box_availability` | string | Add-to-cart availability | `"Add to cart"` / null → bool |
| `delivery_details` | string | Delivery date string | `"Delivery Mon, Sep 1"` — **dropped** (time-bound noise) |
| `sustainability_badges` | string | Sustainability label | `"Carbon impact"` / null; **92% missing** |
| `image_url` | string | Image URL | **dropped** (non-analytical) |
| `product_url` | string | Affiliate-tracked product URL | **dropped** (non-analytical) |
| `collected_at` | string | Scrape timestamp | Cast to datetime |

## Cleaned Schema (21 columns) — `data/processed/amazon_products_clean.csv`
| Column | Type | Description |
|---|---|---|
| `title` | string | Product title (verbatim from raw) |
| `rating` | float | Stars 0–5 |
| `number_of_reviews` | float | Total review count (int-coerced) |
| `bought_in_last_month` | float | Recent monthly purchases |
| `current_discounted_price` | float | Selling price (USD) — may be NaN |
| `listed_price` | float | List price (USD) |
| `is_best_seller` | bool | Has Best Seller / Amazon's Choice badge |
| `is_sponsored` | bool | Sponsored ad slot |
| `has_buy_box` | bool | Add-to-cart available |
| `coupon_pct` | float | Coupon discount % (0 if none, NaN for $-coupons) |
| `has_coupon` | bool | Any coupon present |
| `has_sustainability_badge` | bool | Any sustainability badge |
| `collected_at` | datetime | Scrape timestamp |
| **Engineered:** | | |
| `selling_price` | float | `current_discounted_price` if present, else `listed_price` |
| `discount_pct` | float | `(listed - selling) / listed × 100`; 0 if no discount |
| `is_discounted` | bool | `discount_pct > 0` |
| `brand` | string | **Heuristic:** first token of title (e.g. `"BOYA"` → Boya) |
| `price_tier` | category | Budget (≤$15), Mid (≤$50), Premium (≤$150), Luxury (>$150) |
| `rating_bucket` | category | Poor (<3), Average (<4), Good (<4.5), Excellent (≥4.5) |
| `log_reviews` | float | `log1p(number_of_reviews)` for skew |
| `log_bought_last_month` | float | `log1p(bought_in_last_month)` for skew |

## Cleaning Decisions Log
| Step | Action | Rows Affected |
|---|---|---|
| Standardise column names → snake_case | `current/discounted_price` → `current_discounted_price` | — |
| Parse `rating` ("4.6 out of 5 stars" → 4.6) | regex extract float | all |
| Parse review/buy counts ("6K+", "1,234") | shorthand → numeric | all |
| Parse `listed_price` ("$159.00") | strip currency, comma | all |
| Cap rating to [0,5]; coupon_pct to [0,100] | clip | all |
| Drop rows with no `title` | hard filter | small |
| Drop rows where both prices are NaN | hard filter | ~1.7% |
| Dedupe on `title` (Amazon search returns duplicates across pages) | drop_duplicates | ~85% |
| Drop noise columns | image_url, product_url, delivery_details, etc. | — |

## Known Limitations
- **No category column.** Brand is heuristic (first token of title); category cannot be derived without an external taxonomy. Analysis is **product-level**, not category-level.
- **Single snapshot.** No time series — only cross-sectional analysis (`collected_at` is identical across most rows).
- **Title-based dedupe is aggressive.** Two genuinely different SKUs with identical scraped titles (rare) would collapse.
- **Currency is USD throughout.** Any cleaning that assumed ₹ would be wrong.
- **`price_on_variant` is unstructured text** (`"basic variant price: 2.4GHz"`). Skipped.
- **`bought_in_last_month` is missing for 7.5%** of rows; assume no recent activity, fill with 0 only inside `log_bought_last_month`.

## Update log
- 2026-04-27: Initial dictionary — schema confirmed against raw file profile.
