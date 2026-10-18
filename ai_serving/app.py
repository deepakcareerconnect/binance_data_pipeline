import os
from datetime import date, datetime
from decimal import Decimal
from fastapi import FastAPI, HTTPException
from databricks import sql

app = FastAPI(title="Binance Market Intelligence API")

ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
ALLOWED_WINDOWS = {"1h", "24h", "7d"}

SNAPSHOT_TABLE = "binance_data.gold_ai.asset_snapshot"
COMPARISON_TABLE = "binance_data.gold_analytics.asset_comparison"
RELATIONSHIPS_TABLE = "binance_data.gold_ai.market_relationships"
ALERTS_TABLE = "binance_data.gold_ai.market_alerts"


def json_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def query_gold(query: str, params: tuple | list | None = None):
    hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    with sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token,
    ) as connection:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            columns = [item[0] for item in cursor.description]
            rows = cursor.fetchall()

            return [
                {
                    column: json_value(value)
                    for column, value in zip(columns, row)
                }
                for row in rows
            ]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/market/snapshot")
def market_snapshot():
    rows = query_gold(
        f"""
        SELECT
            symbol,
            last_price,
            return_1m,
            return_5m,
            return_1h,
            return_24h,
            volume_24h_quote,
            volatility_1h,
            trade_count_1m,
            event_timestamp,
            calculated_at,
            data_age_seconds,
            is_stale
        FROM {SNAPSHOT_TABLE}
        ORDER BY symbol
        """
    )

    return {
        "data": rows,
        "count": len(rows),
        "source": SNAPSHOT_TABLE,
    }


@app.get("/market/compare")
def market_compare(window: str = "24h"):
    if window not in {"1h", "24h"}:
        raise HTTPException(
            status_code=400,
            detail="window must be 1h or 24h",
        )

    rows = query_gold(
        f"""
        SELECT
            symbol,
            window,
            period_return,
            volatility,
            performance_rank,
            calculated_through,
            calculated_at
        FROM {COMPARISON_TABLE}
        WHERE window = ?
        ORDER BY performance_rank
        """,
        params=(window,),
    )

    return {"window": window, "data": rows}


@app.get("/market/correlation")
def market_correlation(window: str = "7d"):
    if window not in ALLOWED_WINDOWS:
        raise HTTPException(
            status_code=400,
            detail="window must be 1h, 24h or 7d",
        )

    rows = query_gold(
        f"""
        SELECT
            symbol_a,
            symbol_b,
            window,
            correlation,
            sample_count,
            calculated_through,
            calculated_at
        FROM {RELATIONSHIPS_TABLE}
        WHERE window = ?
        ORDER BY symbol_a, symbol_b
        """,
        params=(window,),
    )

    return {"window": window, "data": rows}


@app.get("/market/alerts")
def market_alerts(symbol: str | None = None):
    if symbol is not None:
        symbol = symbol.upper()
        if symbol not in ALLOWED_SYMBOLS:
            raise HTTPException(
                status_code=400,
                detail="Unsupported symbol",
            )

        query = f"""
            SELECT
                symbol,
                alert_type,
                observed_value,
                threshold,
                severity,
                event_timestamp,
                calculated_at
            FROM {ALERTS_TABLE}
            WHERE symbol = ?
            ORDER BY calculated_at DESC
        """
        rows = query_gold(query, params=(symbol,))
    else:
        query = f"""
            SELECT
                symbol,
                alert_type,
                observed_value,
                threshold,
                severity,
                event_timestamp,
                calculated_at
            FROM {ALERTS_TABLE}
            ORDER BY calculated_at DESC
        """
        rows = query_gold(query)

    return {"data": rows}