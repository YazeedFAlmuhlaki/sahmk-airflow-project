from datetime import datetime

import requests  # type:ignore

from airflow.decorators import dag, task  # type:ignore
from airflow.hooks.base import BaseHook  # type:ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type:ignore


@dag(
    dag_id="fact_daily_prices_dag",
    schedule="@daily",
    catchup=False,
)
def fact_daily_prices_dag():
    """Extract daily Tadawul prices for companies in dim_company."""

    @task
    def extract_company_symbols():
        """Read the company symbols that should be queried from the warehouse."""

        warehouse_hook = PostgresHook(postgres_conn_id="postgres_warehouse")

        company_rows = warehouse_hook.get_records(
            "SELECT symbol FROM dim_company LIMIT 50;"
        )

        return [row[0] for row in company_rows]

    @task
    def extract_daily_market_data(company_symbols):
        """Fetch the current quote for each company symbol from the Sahmk API."""
        api_connection = BaseHook.get_connection("sahmk_api")
        api_key = api_connection.password
        base_url = api_connection.host.rstrip("/")

        date_key = int(datetime.now().strftime("%Y%m%d"))
        request_headers = {"X-API-Key": api_key}
        daily_price_records = []

        for symbol in company_symbols:
            quote_url = f"{base_url}/api/v1/quote/{symbol}/"
            response = requests.get(quote_url, headers=request_headers, timeout=30)
            
            if response.status_code == 200:
                quote_payload = response.json()
                current_price = quote_payload.get("price", 0)
                daily_price_records.append(
                    {
                        "date_key": date_key,
                        "symbol": symbol,
                        "open_price": quote_payload.get("open", current_price),
                        "high_price": quote_payload.get("high", current_price),
                        "low_price": quote_payload.get("low", current_price),
                        "close_price": current_price,
                        "volume": quote_payload.get("volume", 0),
                    }
                )
        return daily_price_records

    @task
    def load_market_data(daily_prices):
        """Upsert daily market prices into the warehouse fact table."""

        warehouse_hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        upsert_prices_sql = """
            INSERT INTO fact_daily_prices (
                date_key,
                symbol,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key, symbol) DO UPDATE
            SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume
        """

        for price_record in daily_prices:
            price_parameters = (
                price_record.get("date_key"),
                price_record.get("symbol"),
                price_record.get("open_price"),
                price_record.get("high_price"),
                price_record.get("low_price"),
                price_record.get("close_price"),
                price_record.get("volume"),
            )

            warehouse_hook.run(upsert_prices_sql, parameters=price_parameters)

    company_symbols = extract_company_symbols()
    daily_prices = extract_daily_market_data(company_symbols)
    load_market_data(daily_prices)


fact_daily_prices_dag()




        