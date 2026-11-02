# Historical Binance Market Data

`data/historical/` contains local-only historical Binance market data used by
the batch ingestion and regression workflows. It is intentionally excluded from
Git because the current local copy is about **537.6 MB across 166 files**.

Do not commit the archives or the extracted CSV files. The directory is covered
by `data/historical/` in `.gitignore`.

## Expected layout

```text
data/historical/
  BTCUSDT/
    BTCUSDT-1m-2025-01.zip
    BTCUSDT-1m-2025-01/
      BTCUSDT-1m-2025-01.csv
  ETHUSDT/
  SOLUSDT/
```

The pipeline supports monthly and daily Binance Vision archives. Keep both the
downloaded `.zip` files and their extracted CSVs when reproducing the existing
local layout.

## Download source

The public source is Binance Vision:

- Monthly klines:
  `https://data.binance.vision/data/spot/monthly/klines/<SYMBOL>/1m/<SYMBOL>-1m-YYYY-MM.zip`
- Daily klines:
  `https://data.binance.vision/data/spot/daily/klines/<SYMBOL>/1m/<SYMBOL>-1m-YYYY-MM-DD.zip`

PowerShell example:

```powershell
$symbol = "BTCUSDT"
$month = "2025-01"
$url = "https://data.binance.vision/data/spot/monthly/klines/$symbol/1m/$symbol-1m-$month.zip"
$target = "data/historical/$symbol/$symbol-1m-$month.zip"
New-Item -ItemType Directory -Force (Split-Path $target) | Out-Null
Invoke-WebRequest -Uri $url -OutFile $target
```

`src/historical/downloader.py` builds the same URLs and uses
`data/historical` as its default download directory.

## Processing locally

After the archives are in place, use the existing historical ingestion entry
points, for example:

```powershell
python -m src.historical.run_historical_batch
```

The historical upload path requires `S3_BUCKET` and `AWS_REGION`, plus normal
AWS credentials in the environment. Never place real credentials in a tracked
file; keep local values in an ignored `.env` file or the process environment.
