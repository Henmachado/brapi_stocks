import os
import logging
import requests
import time

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

API_QUOTE_LIMIT_STOCKS_PER_REQUEST = 10
API_LIST_LIMIT_STOCKS_PER_PAGE = 2000
API_MODULES = [
    None,  # Default API response from /quotes endpoint
    "balanceSheetHistoryQuarterly",
    "cashflowHistoryQuarterly",
    "defaultKeyStatisticsHistoryQuarterly",
    "incomeStatementHistoryQuarterly",
    "financialDataHistoryQuarterly",
    "valueAddedHistoryQuarterly",
]
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
        else:
            logger.error(f"Failed to fetch page {page}: {page_response.status_code}")

    return all_stocks


def get_api_stock_data_per_module(ticker_list: list[str], module: str = None) -> list[dict]:
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

    if module is not None:
        selected_keys = {"symbol", module}
        filtered_results = [
            {k: v for k, v in item.items() if k in selected_keys}
            for item in results
            if isinstance(item, dict)  # Defensive check against malformed API data
        ]

        return filtered_results  # Return only relevant module data

    return results


def fetch_api_data_per_ticker_batch(stock_list: list[str], module: str = None) -> list[dict]:
    data_results = []
    for i in range(0, len(stock_list), API_QUOTE_LIMIT_STOCKS_PER_REQUEST):
        # Loop through the tickers in chunks according API limits

        batch = stock_list[i:i + API_QUOTE_LIMIT_STOCKS_PER_REQUEST]
        logger.info(
            f"Getting stock data | Module: {module} | "
            f"Fetching batch {i // API_QUOTE_LIMIT_STOCKS_PER_REQUEST + 1}"
        )

        batch_response = get_api_stock_data_per_module(ticker_list=batch, module=module)
        data_results.extend(batch_response)

    logger.error(f"\nSuccessfully retrieved data for {len(data_results)}/{len(stock_list)} stocks for {module}\n")

    return data_results