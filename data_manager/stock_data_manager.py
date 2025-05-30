# backtest/data_manager/stock_data_manager.py

import logging
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db.db_manager import DBManager
from api_client.creon_api import CreonAPIClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockDataManager:
    def __init__(self):
        self.db_manager = DBManager()
        self.creon_api_client = CreonAPIClient()
        if not self.creon_api_client.connected:
            logger.error("Creon API client is not connected. StockDataManager might not function correctly.")

    def initialize_stock_info(self, force_update=False): # force_update 인자 유지
        """
        Creon API에서 모든 종목 정보를 가져와 DB에 저장합니다.
        이미 데이터가 존재하면 업데이트하지 않지만, force_update=True면 강제로 업데이트합니다.
        """
        logger.info("Initializing stock information in DB...")
        if not self.creon_api_client.connected:
            logger.error("Creon API is not connected. Cannot initialize stock info.")
            return False

        existing_stock_count = self.db_manager.get_stock_info_count()
        # force_update가 True면 강제로 업데이트하고, 아니면 기존 데이터가 있을 경우 스킵
        if existing_stock_count > 0 and not force_update:
            logger.info(f"Stock info already exists in DB ({existing_stock_count} records). Skipping initialization. Use force_update=True to force update.")
            return False
        
        # force_update가 True이거나, 기존 데이터가 없는데 처음 초기화하는 경우
        if force_update or existing_stock_count == 0:
            if existing_stock_count > 0: # 기존 데이터가 있는데 force_update인 경우만 삭제
                logger.info("Force update requested or no existing data. Deleting existing stock info (if any)...")
                self.db_manager.delete_all_stock_info() # 모든 데이터 삭제
            else:
                logger.info("No existing stock info found. Proceeding with initial population.")

        filtered_codes = self.creon_api_client.get_filtered_stock_list()
        stock_info_list = []
        for code in filtered_codes:
            name = self.creon_api_client.get_stock_name(code)
            market_type_int = self.creon_api_client.cp_code_mgr.GetStockMarketKind(code)
            market_type_str = "KOSPI" if market_type_int == 1 else ("KOSDAQ" if market_type_int == 2 else "기타")

            stock_info_list.append({
                'stock_code': code,
                'stock_name': name,
                'market_type': market_type_str,
                'sector': 'N/A',
                'per': 0.0,
                'pbr': 0.0,
                'eps': 0.0
            })

        if stock_info_list:
            logger.info(f"Saving {len(stock_info_list)} stock info records to DB.")
            self.db_manager.save_stock_info(stock_info_list)
            logger.info("Stock info initialization complete.")
            return True
        else:
            logger.warning("No stock information retrieved from Creon API for initialization.")
            return False

    def update_daily_ohlcv_data(self, stock_code=None, start_date=None, end_date=None):
        """
        특정 종목 또는 모든 종목의 일봉 데이터를 Creon API에서 가져와 DB에 저장/업데이트합니다.
        """
        logger.info("Updating daily OHLCV data...")
        if not self.creon_api_client.connected:
            logger.error("Creon API is not connected. Cannot update daily data.")
            return False

        if end_date is None:
            end_date = datetime.now().date()

        target_codes = []
        if stock_code:
            target_codes.append(stock_code)
        else:
            target_codes = self.creon_api_client.get_filtered_stock_list()
            logger.info(f"Fetching daily data for {len(target_codes)} filtered stocks.")

        total_saved_records = 0
        for code in target_codes:
            latest_date_in_db = self.db_manager.get_latest_daily_data_date(code)

            current_start_date = start_date
            if current_start_date is None and latest_date_in_db:
                current_start_date = latest_date_in_db + timedelta(days=1)
            elif current_start_date is None:
                current_start_date = datetime(2000, 1, 1).date()

            if current_start_date > end_date:
                logger.info(f"Daily data for {code} is already up-to-date or start date is after end date. Skipping.")
                continue

            start_date_str = current_start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')

            logger.info(f"Fetching daily OHLCV for {code} from {start_date_str} to {end_date_str}")
            daily_df = self.creon_api_client.get_daily_ohlcv(code, start_date_str, end_date_str)

            if not daily_df.empty:
                daily_data_to_save = daily_df.to_dict(orient='records')
                saved_count = self.db_manager.save_daily_data(daily_data_to_save)
                total_saved_records += saved_count
                logger.info(f"Saved {saved_count} daily records for {code}.")
            else:
                logger.warning(f"No daily data retrieved for {code} in the specified period.")

        logger.info(f"Total daily records saved/updated: {total_saved_records}.")
        return total_saved_records > 0

    def update_minute_ohlcv_data(self, stock_code=None, start_date=None, end_date=None): # interval 인자 제거 (1분봉 고정)
        """
        특정 종목 또는 모든 종목의 1분봉 데이터를 Creon API에서 가져와 DB에 저장/업데이트합니다.
        :param stock_code: 특정 종목 코드. None이면 모든 종목 (권장하지 않음).
        :param start_date: 조회 시작 날짜 (datetime.date 객체). None이면 DB의 마지막 날짜 다음 날부터.
        :param end_date: 조회 종료 날짜 (datetime.date 객체). None이면 오늘 날짜.
        """
        interval = 1 # 1분봉 고정 (Creon API 요청에는 여전히 사용)
        logger.info(f"Updating {interval}-minute OHLCV data...")
        if not self.creon_api_client.connected:
            logger.error("Creon API is not connected. Cannot update minute data.")
            return False

        if end_date is None:
            end_date = datetime.now().date()
        
        target_codes = []
        if stock_code:
            target_codes.append(stock_code)
        else:
            target_codes = self.creon_api_client.get_filtered_stock_list()
            logger.warning(f"Updating minute data for all {len(target_codes)} filtered stocks. This might take a very long time and hit API limits.")

        total_saved_records = 0
        for code in target_codes:
            # get_latest_minute_data_datetime 호출 시 interval 인자 제거
            latest_datetime_in_db = self.db_manager.get_latest_minute_data_datetime(code) 

            current_start_datetime = None
            if latest_datetime_in_db:
                current_start_datetime = latest_datetime_in_db + timedelta(minutes=interval)
            else:
                current_start_datetime = datetime.combine(start_date, datetime.min.time().replace(hour=9)) if start_date else datetime(2000, 1, 1, 9, 0)

            current_end_datetime = datetime.combine(end_date, datetime.max.time())
            
            if current_start_datetime > current_end_datetime:
                logger.info(f"Minute data for {code} is already up-to-date or start datetime is after end datetime. Skipping.")
                continue

            start_date_str = current_start_datetime.strftime('%Y%m%d')
            end_date_str = current_end_datetime.strftime('%Y%m%d')

            logger.info(f"Fetching {interval}-minute OHLCV for {code} from {start_date_str} to {end_date_str}")
            # creon_api_client.get_minute_ohlcv 호출 시 interval 인자 유지 (Creon API 요청에는 필요)
            minute_df = self.creon_api_client.get_minute_ohlcv(code, start_date_str, end_date_str, interval)

            if not minute_df.empty:
                minute_data_to_save = minute_df.to_dict(orient='records')
                saved_count = self.db_manager.save_minute_data(minute_data_to_save)
                total_saved_records += saved_count
                logger.info(f"Saved {saved_count} minute records for {code}.")
            else:
                logger.warning(f"No minute data retrieved for {code} in the specified period.")
        
        logger.info(f"Total minute records saved/updated: {total_saved_records}.")
        return total_saved_records > 0