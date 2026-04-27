# Tableau Dashboard Links

## Data source
Tableau reads directly from `data/processed/amazon_products_clean.csv` (5,971 rows × 21 cols, zero NaN).
No separate extract file is maintained — the cleaned dataset is the single source of truth.

## Public Dashboard
- **URL:** _to be added after publishing to Tableau Public_
- **Title:** Amazon Products Portfolio Analytics — Pricing, Ratings & Discoverability
- **Last updated:** _YYYY-MM-DD_

## Views
1. **Executive Summary** — KPI cards (catalogue size, avg price, avg rating, total reviews), top categories, price tier mix
2. **Category Drill-Down** — interactive filter on category → price distribution, rating distribution, top brands
3. **Pricing & Discount Analysis** — price vs rating scatter, discount-band performance, price-tier × rating-bucket heatmap
4. **Brand Leaderboard** — top brands by review volume, avg rating, price positioning

## Required Interactive Elements
- [ ] Category filter (multi-select)
- [ ] Price-tier filter
- [ ] Rating-bucket filter
- [ ] Brand search/filter
- [ ] Tooltips with product-level context

## Screenshots
Stored under `tableau/screenshots/`:
- `01_executive_summary.png`
- `02_category_drilldown.png`
- `03_pricing_discount.png`
- `04_brand_leaderboard.png`
