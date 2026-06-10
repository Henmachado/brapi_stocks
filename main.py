import logging
import argparse
import utils
from concurrent.futures import ThreadPoolExecutor
from api_ingestion import brapi_api_consumer

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)



def ingest_api_module_data(stock_list: list[str], module: str = None):
    """Worker function to fetch and save data for a specific module."""
    api_response = brapi_api_consumer.fetch_api_data_per_ticker_batch(stock_list=stock_list, module=module)
    file_name = module.lower() if module else "defaultquoteapi"
    utils.save_json_data(data=api_response, file_name=file_name)
    return file_name


if __name__ == "__main__":
    logger.info(f"\n Starting Stock Data Ingestion \n")

    parser = argparse.ArgumentParser(description="Brapi Stock Data Ingestion")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-free", action="store_true", help="Ingest only free stock tickers")
    group.add_argument("-full-api", action="store_true", help="Ingest all active stock tickers")

    args = parser.parse_args()

    logger.info(f"\n Fetching all active stock tickers \n")
    active_tickers_response = brapi_api_consumer.get_active_stock_tickers()
    utils.save_json_data(data=active_tickers_response, file_name=f"activetickers")

    active_tickers_list: list[str] = [stock['stock'] for stock in active_tickers_response]
    logger.info(f"Total Active Stocks Found: {len(active_tickers_list)} \n")

    if args.full_api:
        logger.info("Running in FULL mode: Ingesting all active tickers.\n")
        _stock_list = active_tickers_list
    else:
        logger.info("Running in FREE mode: Ingesting default free tickers.\n")
        _stock_list = brapi_api_consumer.FREE_STOCKS_TICKERS

    # 2. Parallel Ingestion for all modules and default quotes
    with ThreadPoolExecutor(max_workers=20) as executor:
        # Prepare the list of tasks (modules + None for default quotes)
        tasks = brapi_api_consumer.API_MODULES

        logger.info(f"Starting parallel ingestion for {len(tasks)} tasks...\n")

        futures = [executor.submit(ingest_api_module_data, _stock_list, task) for task in tasks]

        for future in futures:
            completed_file = future.result()

    logger.info("\nAll data ingestion tasks completed successfully.")
