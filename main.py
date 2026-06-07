import logging
import argparse
import utils
from api_ingestion import brapi_api_consumer

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Brapi Stock Data Ingestion")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-free", action="store_true", help="Ingest only free stock tickers")
    group.add_argument("-full-api", action="store_true", help="Ingest all active stock tickers")
    
    args = parser.parse_args()

    # Fetch all active stock tickers (~2,280 items on jun/2026)
    active_tickers_response = brapi_api_consumer.get_active_stock_tickers()

    utils.save_json_data(data=active_tickers_response, file_name=f"activetickers")

    active_tickers_list: list[str] = [stock['stock'] for stock in active_tickers_response]
    logger.info(f"Total Active Stocks Found: {len(active_tickers_list)} \n")

    # Set stock_list based on CLI arguments
    if args.full_api:
        logger.info("Running in FULL mode: Ingesting all active tickers.\n")
        stock_list = active_tickers_list
    else:
        logger.info("Running in FREE mode: Ingesting default free tickers.\n")
        stock_list = brapi_api_consumer.FREE_STOCKS_TICKERS

    # Fetch data for all available modules
    # for _module in brapi_api_consumer.API_MODULES:
    #     api_response = brapi_api_consumer.fetch_api_data_per_ticker_batch(stock_list=stock_list, module=_module)
    #     utils.save_json_data(data=api_response, file_name=_module.lower())

    # Fetch data for default api response (no module passed on request)
    api_response = brapi_api_consumer.fetch_api_data_per_ticker_batch(stock_list=stock_list)
    utils.save_json_data(data=api_response, file_name="defaultquoteapi")
