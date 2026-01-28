# Free Hosted Demo: Streamlit Cloud + Neon/Supabase + GitHub Actions

This guide sets up a free hosted demo with:
- Streamlit Community Cloud for the dashboard
- Neon or Supabase for hosted Postgres
- GitHub Actions for scheduled ETL runs

## 1) Create a hosted Postgres database

### Option A: Neon (recommended for quick setup)
1. Create a Neon project and database.
2. Copy the connection string (Postgres URL).
3. Keep it handy for Streamlit and GitHub Actions secrets.

### Option B: Supabase
1. Create a Supabase project.
2. Go to the project settings and find the Postgres connection string.
3. Use the direct Postgres URL (not the pooled URL) for best compatibility.

Your URL will look like this:
```
postgresql://USER:PASSWORD@HOST:5432/DBNAME
```

## 2) Streamlit Cloud setup

1. Push this repo to GitHub.
2. In Streamlit Cloud, deploy `streamlit_app.py`.
3. Add a Streamlit secret:

```
DATABASE_URL="postgresql://USER:PASSWORD@HOST:5432/DBNAME"
```

## 3) GitHub Actions cron setup

Add GitHub repository secrets:

- `DATABASE_URL` → your hosted Postgres URL
- `COINGECKO_API_KEY` (optional)
- `KRAKEN_API_KEY` (optional)

The workflow file is already included at:
`.github/workflows/etl.yml`

By default it runs hourly. You can edit the cron schedule if you want a different cadence.

## 4) Verify it’s working

1. Trigger the workflow manually (`workflow_dispatch`) once.
2. Check Streamlit dashboard for new data.
3. Optional: use Azure Data Studio to connect to the hosted DB and verify:

```
SELECT COUNT(*) FROM fact_price_candle;
```

## Notes

- Streamlit Cloud needs a public Postgres URL. Localhost will not work.
- GitHub Actions runs in the cloud, so it also needs the public DB URL.
- If you see rate limits, reduce the cron frequency or limit the API window.
