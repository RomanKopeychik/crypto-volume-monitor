import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable


TICKERS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
BASE = "https://data-api.binance.vision/api/v3/klines"

logger = logging.getLogger(__name__)


@dag(
    schedule=CronDataIntervalTimetable("*/5 * * * *", timezone="Europe/Moscow"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["crypto"],
)
def crypto_volume():

    @task()
    def extract():
        import requests

        context = get_current_context()
        start = context["data_interval_start"]
        end = context["data_interval_end"]

        logger.info(f"Загружаем данные за {start} — {end}")

        rows = []

        for symbol in TICKERS:
            r = requests.get(
                BASE,
                params={
                    "symbol": symbol,
                    "interval": "5m",
                    "startTime": int(start.timestamp() * 1000),
                    "endTime": int(end.timestamp() * 1000) - 1,
                    "limit": 1,
                },
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()

            if not data:
                raise ValueError(f"Нет данных для {symbol}")

            candle = data[0]

            ts = pendulum.from_timestamp(
                int(candle[0]) / 1000, tz="UTC"
            ).isoformat()

            volume_base = float(candle[5])
            volume_usdt = float(candle[7])
            price = float(candle[4])

            if volume_base < 0 or volume_usdt < 0 or price <= 0:
                raise ValueError(f"Некорректные данные для {symbol}")

            rows.append(
                (ts, symbol, volume_base, volume_usdt, price)
            )

            logger.info(
                f"{symbol}: объём торгов = {volume_usdt} USDT"
            )

        return rows

    @task()
    def load(rows: list):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if len(rows) != len(TICKERS):
            raise ValueError("Получены данные не по всем монетам")

        hook = PostgresHook(postgres_conn_id="bi_db")
        hook.insert_rows(
            table="crypto_volume",
            rows=[tuple(row) for row in rows],
            target_fields=[
                "ts", "symbol", "volume_base", "volume_usdt", "price",
            ],
            replace=True,
            replace_index=["ts", "symbol"],
        )

        logger.info(f"Записано строк: {len(rows)}")

    load(extract())


crypto_volume()
