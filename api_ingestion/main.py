import constants
import json
import os
import logging
import requests
import time

from datetime import datetime
from dotenv import load_dotenv
from typing import Any

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

API_QUOTE_LIMIT_STOCKS_PER_REQUEST = 10
API_LIST_LIMIT_STOCKS_PER_PAGE = 2000
RATE_LIMIT_WAIT = 1.0
FREE_STOCKS_TICKERS = ["PETR4", "MGLU3", "VALE3", "ITUB4"]
TOKEN = os.getenv("TOKEN")


def get_active_stock_tickers() -> list[dict]:
    """Fetches all active stock tickers from Brapi."""
    url = f"https://brapi.dev/api/quote/list?page=1&limit={API_LIST_LIMIT_STOCKS_PER_PAGE}"
    initial_response = requests.request(method="GET", url=url, params={"token": TOKEN})

    if initial_response.status_code != 200:
        logger.error(f"Failed to fetch active tickers: {initial_response.status_code}")
        return []

    # Parse JSON once
    initial_data = initial_response.json()
    total_pages = initial_data.get("totalPages", 1)
    stocks_page_1 = initial_data.get('stocks', [])

    logger.info(f"Fetching page 1/{total_pages} | Stock Tickers: {len(stocks_page_1)}")

    # Initialize data list with page 1 results
    all_stocks = list(stocks_page_1)

    for page in range(2, total_pages + 1):
        page_url = f"https://brapi.dev/api/quote/list?page={page}&limit={API_LIST_LIMIT_STOCKS_PER_PAGE}"
        page_response = requests.request(method="GET", url=page_url, params={"token": TOKEN})

        if page_response.status_code == 200:
            page_data = page_response.json()
            page_stocks = page_data.get('stocks', [])
            logger.info(f"Fetching page {page}/{total_pages} | Stock Tickers: {len(page_stocks)}")
            all_stocks.extend(page_stocks)
            time.sleep(RATE_LIMIT_WAIT)  # Avoid firewall and rate limits in case of multiple pages
        else:
            logger.error(f"Failed to fetch page {page}: {page_response.status_code}")

    return all_stocks


def save_json_data(data: Any, file_name: str, storage_dir: str = constants.RAW_LAYER) -> None:
    os.makedirs(storage_dir, exist_ok=True)
    file_path = os.path.join(storage_dir, f"{file_name}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def get_api_stock_data_per_module(ticker_list: list[str], module: str) -> list[dict]:
    if ticker_list is None:
        logger.info(f"Fetching stock data for only free tickers: {FREE_STOCKS_TICKERS}")
        ticker_list = FREE_STOCKS_TICKERS

    if len(ticker_list) > API_QUOTE_LIMIT_STOCKS_PER_REQUEST:
        logger.error(f"API only supports a maximum of {API_QUOTE_LIMIT_STOCKS_PER_REQUEST} tickers per request.")
        return []

    url = f"https://brapi.dev/api/quote/{','.join(ticker_list)}"
    params = {"token": TOKEN, "modules": module}
    response = requests.request(method="GET", url=url, params=params)

    if response.status_code != 200:
        logger.error(f"Failed to fetch data for batch {ticker_list}: {response.status_code}")
        return []

    results = response.json().get("results", [])

    if module != "defaultKeyStatistics":
        selected_keys = {"symbol", module}
        filtered_results = [
            {k: v for k, v in item.items() if k in selected_keys}
            for item in results
            if isinstance(item, dict)  # Defensive check against malformed API data
        ]

        return filtered_results  # Return only relevant module data

    return results


def fetch_api_data_per_ticker_batch(stock_list: list[str], module: str) -> list[dict]:
    logger.info(f"Getting stock data | Module: {module}")

    data_results = []
    for i in range(0, len(stock_list), API_QUOTE_LIMIT_STOCKS_PER_REQUEST):
        # Loop through the tickers in chunks according API limits

        batch = stock_list[i:i + API_QUOTE_LIMIT_STOCKS_PER_REQUEST]
        logger.info(f"Fetching batch {i // API_QUOTE_LIMIT_STOCKS_PER_REQUEST + 1}: {', '.join(batch)}")

        batch_response = get_api_stock_data_per_module(ticker_list=batch, module=module)
        data_results.extend(batch_response)

        time.sleep(RATE_LIMIT_WAIT) # Avoid firewall and rate limits

    logger.error(f"Successfully retrieved data for {len(data_results)}/{len(stock_list)} stocks.\n")

    return data_results


if __name__ == "__main__":
    today_str = datetime.now().strftime("%Y_%m_%d")

    # Fetch all active stock tickers (~2,280 items on jun/2026)
    active_tickers_response = get_active_stock_tickers()

    save_json_data(
        data=active_tickers_response,
        file_name=f"active_tickers_{today_str}"
    )

    active_tickers_list: list[str] = [stock['stock'] for stock in active_tickers_response]
    logger.info(f"Total Active Stocks Found: {len(active_tickers_list)} \n")

    # -----------------------------------------------------------------

    # Fetch defaultKeyStatistics
    api_response = fetch_api_data_per_ticker_batch(
        stock_list=FREE_STOCKS_TICKERS,
        module="defaultKeyStatistics",
    )
    save_json_data(
        data=api_response,
        file_name=f"defaultkeystatistics_{today_str}"
    )

    # Quarterly Modules -----------------------------------------------

    # Fetch balanceSheetHistoryQuarterly
    api_response = fetch_api_data_per_ticker_batch(
        stock_list=FREE_STOCKS_TICKERS,
        module="balanceSheetHistoryQuarterly",
    )
    save_json_data(
        data=api_response,
        file_name=f"balancesheethistoryquarterly_{today_str}"
    )

    # -----------------------------------------------------------------

    # Fetch cashflowHistoryQuarterly
    api_response = fetch_api_data_per_ticker_batch(
        stock_list=FREE_STOCKS_TICKERS,
        module="cashflowHistoryQuarterly",
    )
    save_json_data(
        data=api_response,
        file_name=f"cashflowhistoryquarterly_{today_str}"
    )

    # -----------------------------------------------------------------

    # Fetch defaultKeyStatisticsHistoryQuarterly
    api_response = fetch_api_data_per_ticker_batch(
        stock_list=FREE_STOCKS_TICKERS,
        module="defaultKeyStatisticsHistoryQuarterly",
    )
    save_json_data(
        data=api_response,
        file_name=f"defaultkeystatisticshistoryquarterly_{today_str}"
    )

    # -----------------------------------------------------------------

    # Fetch incomeStatementHistoryQuarterly
    api_response = fetch_api_data_per_ticker_batch(
        stock_list=FREE_STOCKS_TICKERS,
        module="incomeStatementHistoryQuarterly",
    )
    save_json_data(
        data=api_response,
        file_name=f"incomestatementhistoryquarterly_{today_str}"
    )

    # -----------------------------------------------------------------

    # Fetch financialDataHistoryQuarterly
    api_response = fetch_api_data_per_ticker_batch(
        stock_list=FREE_STOCKS_TICKERS,
        module="financialDataHistoryQuarterly",
    )
    save_json_data(
        data=api_response,
        file_name=f"financialdatahistoryquarterly_{today_str}"
    )

    # -----------------------------------------------------------------

    # Fetch valueAddedHistoryQuarterly
    api_response = fetch_api_data_per_ticker_batch(
        stock_list=FREE_STOCKS_TICKERS,
        module="valueAddedHistoryQuarterly",
    )
    save_json_data(
        data=api_response,
        file_name=f"valueaddedhistoryquarterly_{today_str}"
    )

    # -----------------------------------------------------------------
