# Crypto Pipeline (ETL + Trust Dashboard)

A simple end‑to‑end ETL pipeline that ingests crypto market data (CoinGecko + Kraken),
cleans and standardizes it, loads it into Postgres, and serves an action‑oriented
Streamlit dashboard.

## What it does
- **Extract:** Pulls raw data from two sources with basic resiliency.
- **Transform:** Normalizes schema, enforces intervals, removes duplicates, flags anomalies.
- **Load:** Writes analytics‑ready candles into Postgres.
- **Visualize:** Streamlit dashboard for freshness, quality, and divergence insights.

## Local setup
```bash
source venv/bin/activate
python -m pip install -r requirements.txt
```

Create `.env` with your Postgres URL:
```
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
```

## Run the ETL
```bash
python src/extract.py
python src/transform.py
python src/load.py
```

## Run the dashboard (local)
```bash
streamlit run streamlit_app.py
```

## Hosted demo (free)
Use **Streamlit Community Cloud + Supabase/Neon + GitHub Actions**.
See `docs/hosting.md` for the step‑by‑step setup.

## Repo structure
- `src/extract.py` — fetches raw data and writes CSVs
- `src/transform.py` — standardizes, resamples, flags anomalies
- `src/load.py` — loads into Postgres
- `streamlit_app.py` — dashboard
- `.github/workflows/etl.yml` — scheduled ETL (cron)
- `docs/hosting.md` — free hosting guide

## Notes
- Do not commit `.env` or any secrets.
- For hosted runs, set `DATABASE_URL` in Streamlit and GitHub Actions secrets.
