import requests  # type:ignore

from airflow.decorators import dag, task  # type:ignore
from airflow.hooks.base import BaseHook  # type:ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type:ignore


# Weekly sync of company data from the external API into the warehouse dimension table.
@dag(
    dag_id="dim_company_dag",
    schedule="@weekly",  
    catchup=False,
     tags=["dimension", "company"], 
)
def dim_company_dag():
    """Load company names from the API into the company dimension table."""

    @task
    def extract_companies():
        """Fetch all company records from the API using pagination."""
        api_connection = BaseHook.get_connection("sahmk_api")
        api_key = api_connection.password
        base_url = api_connection.host.rstrip("/")
        companies_url = f"{base_url}/api/v1/companies/"

        page_limit = 100
        page_offset = 0

        headers = {"X-API-Key": api_key}
        params = {"limit": page_limit, "offset": page_offset}

        all_companies = []
        total_records = 1

        # Continue fetching pages until the API reports that no more rows remain.
        while page_offset < total_records:
            response = requests.get(companies_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            payload = response.json()

            total_records = payload["total"]
            all_companies.extend(payload["results"])

            page_offset += page_limit
            params["offset"] = page_offset

        return all_companies

    @task
    def load_companies(company_records):
        """Insert or update company records in the warehouse table."""
        postgres_hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        sql_query = """
            INSERT INTO dim_company (
                symbol,
                name_ar,
                name_en,
                market,
                market_segment,
                security_type,
                status,
                is_etf
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE
            SET
                name_ar = EXCLUDED.name_ar,
                name_en = EXCLUDED.name_en,
                market = EXCLUDED.market,
                market_segment = EXCLUDED.market_segment,
                security_type = EXCLUDED.security_type,
                status = EXCLUDED.status,
                is_etf = EXCLUDED.is_etf
        """

        for company in company_records:
            company_data = (
                company.get("symbol"),
                company.get("name_ar"),
                company.get("name_en"),
                company.get("market"),
                company.get("market_segment"),
                company.get("security_type"),
                company.get("status"),
                company.get("is_etf"),
            )
            postgres_hook.run(sql_query, parameters=company_data)

    extracted_companies = extract_companies()
    load_companies(extracted_companies)


dim_company_dag()