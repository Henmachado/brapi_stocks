import logging
import utils
from api_ingestion import brapi_api_consumer

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    # Fetch all active stock tickers (~2,280 items on jun/2026)
    active_tickers_response = brapi_api_consumer.get_active_stock_tickers()

    utils.save_json_data(data=active_tickers_response, file_name=f"activetickers")

    active_tickers_list: list[str] = [stock['stock'] for stock in active_tickers_response]
    logger.info(f"Total Active Stocks Found: {len(active_tickers_list)} \n")

    # To ingest all available modules, this behaviour is set as default.
    # You will need to have a pro api plan to ingest all modules for all
    # public available stocks.

    stock_list = brapi_api_consumer.FREE_STOCKS_TICKERS  # replace for active_tickers_list in pro api plan

    for _module in brapi_api_consumer.API_MODULES:
        api_response = brapi_api_consumer.fetch_api_data_per_ticker_batch(stock_list=stock_list, module=_module)
        utils.save_json_data(data=api_response, file_name=_module.lower())
