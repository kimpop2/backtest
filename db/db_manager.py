# backtest/db/db_manager.py

import pymysql
from dotenv import load_dotenv
import os
from config.settings import DB_HOST, DB_PORT, DB_NAME
import logging
import pandas as pd
from datetime import datetime

# .env 파일에서 환경 변수 로드
load_dotenv()

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self):
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_host = DB_HOST
        self.db_port = DB_PORT
        self.db_name = DB_NAME

        if not self.db_user or not self.db_password:
            logger.error("DB_USER or DB_PASSWORD not found in .env file.")
            raise ValueError("Database credentials are not set in .env file.")

    def get_db_connection(self):
        """MariaDB 데이터베이스 연결을 반환합니다."""
        try:
            conn = pymysql.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                db=self.db_name,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor # 딕셔너리 형태로 결과 반환
            )
            logger.info(f"Successfully connected to MariaDB: {self.db_name}")
            return conn
        except pymysql.Error as e:
            logger.error(f"Error connecting to MariaDB: {e}")
            raise

    def execute_query(self, query, params=None, fetch_one=False, fetch_all=False):
        """SQL 쿼리를 실행하고 결과를 반환합니다."""
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit() # INSERT, UPDATE, DELETE 시 커밋
                if fetch_one:
                    return cursor.fetchone()
                if fetch_all:
                    return cursor.fetchall()
                return cursor.rowcount # INSERT, UPDATE, DELETE 시 영향 받은 행 수 반환
        except pymysql.Error as e:
            logger.error(f"Error executing query: {query} with params {params}. Error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def insert_data(self, table_name, data):
        """
        단일 레코드 또는 여러 레코드를 테이블에 삽입합니다.
        :param table_name: 데이터를 삽입할 테이블 이름
        :param data: 딕셔너리 (단일 레코드) 또는 딕셔너리 리스트 (여러 레코드)
        """
        if not data:
            return 0

        if isinstance(data, dict):
            data_list = [data]
        else:
            data_list = data

        if not data_list:
            return 0

        columns = ', '.join(data_list[0].keys())
        placeholders = ', '.join(['%s'] * len(data_list[0]))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        rows_affected = 0
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cursor:
                for record in data_list:
                    values = tuple(record.values())
                    cursor.execute(query, values)
                    rows_affected += 1
                conn.commit()
            logger.info(f"Successfully inserted {rows_affected} record(s) into {table_name}.")
            return rows_affected
        except pymysql.Error as e:
            logger.error(f"Error inserting data into {table_name}. Data: {data_list}. Error: {e}")
            if conn:
                conn.rollback() # 오류 발생 시 롤백
            raise
        finally:
            if conn:
                conn.close()

    def fetch_data(self, table_name, conditions=None, columns='*', order_by=None, limit=None):
        """
        테이블에서 데이터를 조회합니다.
        :param table_name: 데이터를 조회할 테이블 이름
        :param conditions: 딕셔너리 형태의 WHERE 조건 (예: {'stock_code': 'A005930'})
        :param columns: 조회할 컬럼 (문자열, 기본값 '*')
        :param order_by: 정렬 기준 (문자열, 예: 'date DESC')
        :param limit: 조회할 레코드 수 제한 (정수)
        :return: 조회된 데이터를 Pandas DataFrame으로 반환
        """
        query = f"SELECT {columns} FROM {table_name}"
        params = []
        if conditions:
            where_clauses = []
            for col, val in conditions.items():
                if isinstance(val, (list, tuple)): # IN 절 처리
                    placeholders = ', '.join(['%s'] * len(val))
                    where_clauses.append(f"{col} IN ({placeholders})")
                    params.extend(val)
                else:
                    where_clauses.append(f"{col} = %s")
                    params.append(val)
            query += " WHERE " + " AND ".join(where_clauses)
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"

        conn = None
        try:
            conn = self.get_db_connection()
            df = pd.read_sql(query, conn, params=params)
            logger.info(f"Successfully fetched {len(df)} records from {table_name}.")
            return df
        except pymysql.Error as e:
            logger.error(f"Error fetching data from {table_name}. Query: {query}. Error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    # 각 테이블에 특화된 조회 및 저장 함수 (예시)
    def save_stock_info(self, stock_info_list):
        """
        종목 정보를 stock_info 테이블에 저장 (UPSERT 기능 추가 예정)
        :param stock_info_list: 종목 정보 딕셔너리 리스트
        """
        # 일단 단순 INSERT로 구현, 추후 UPSERT 또는 INSERT IGNORE 로직 추가
        logger.info(f"Attempting to save {len(stock_info_list)} stock info records.")
        return self.insert_data('stock_info', stock_info_list)

    def fetch_stock_info(self, stock_codes=None):
        """
        stock_info 테이블에서 종목 정보를 조회합니다.
        :param stock_codes: 특정 종목 코드 리스트 (선택적)
        """
        conditions = {'stock_code': stock_codes} if stock_codes else None
        return self.fetch_data('stock_info', conditions=conditions)

    def save_daily_data(self, daily_data_list):
        """
        일봉 데이터를 daily_stock_data 테이블에 저장합니다.
        :param daily_data_list: 일봉 데이터 딕셔너리 리스트
        """
        logger.info(f"Attempting to save {len(daily_data_list)} daily data records.")
        return self.insert_data('daily_stock_data', daily_data_list)

    def fetch_daily_data(self, stock_code, start_date=None, end_date=None):
        """
        daily_stock_data 테이블에서 특정 종목의 일봉 데이터를 조회합니다.
        :param stock_code: 조회할 종목 코드
        :param start_date: 시작 날짜 (datetime.date 또는 문자열 'YYYY-MM-DD')
        :param end_date: 종료 날짜 (datetime.date 또는 문자열 'YYYY-MM-DD')
        :return: Pandas DataFrame
        """
        query = f"SELECT * FROM daily_stock_data WHERE stock_code = %s"
        params = [stock_code]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)

        query += " ORDER BY date ASC" # 날짜 순으로 정렬하여 반환

        conn = None
        try:
            conn = self.get_db_connection()
            df = pd.read_sql(query, conn, params=params)
            # 'date' 컬럼을 datetime 객체로 변환하여 Backtrader 호환성 높임
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            logger.info(f"Fetched {len(df)} daily records for {stock_code}.")
            return df
        except pymysql.Error as e:
            logger.error(f"Error fetching daily data for {stock_code}. Error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def save_backtest_result(self, result_data):
        """백테스팅 결과를 backtest_results 테이블에 저장합니다."""
        logger.info(f"Attempting to save backtest result: {result_data['strategy_name']}")
        return self.insert_data('backtest_results', result_data)

    def save_trade_log(self, trade_logs):
        """거래 로그를 trade_log 테이블에 저장합니다."""
        logger.info(f"Attempting to save {len(trade_logs)} trade logs.")
        return self.insert_data('trade_log', trade_logs)


    def get_stock_info_count(self):
        """stock_info 테이블의 총 레코드 개수를 반환합니다."""
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return 0
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM stock_info")
            result = cursor.fetchone() # 결과를 먼저 변수에 할당
            if result is not None: # 결과가 None이 아닌지 확인
                count = result[0]
                logger.info(f"Current stock_info table has {count} records.")
                return count
            else: # 결과가 None인 경우 (예외적인 상황이지만 대비)
                logger.warning("No result from COUNT(*) query for stock_info table. Assuming 0 records.")
                return 0
        except Exception as e:
            logger.error(f"Failed to get stock info count: {e}", exc_info=True)
            return 0
        finally:
            if conn:
                conn.close()

    def get_latest_daily_data_date(self, stock_code):
        """
        특정 종목의 daily_stock_data 테이블에서 가장 최근 날짜를 조회합니다.
        :param stock_code: 조회할 종목 코드
        :return: datetime.date 객체 또는 None
        """
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return None
            cursor = conn.cursor()
            query = "SELECT MAX(date) FROM daily_stock_data WHERE stock_code = %s"
            cursor.execute(query, (stock_code,))
            result = cursor.fetchone() # 결과를 먼저 변수에 할당
            if result is not None and result[0] is not None: # 결과가 None이 아니고, 첫 번째 컬럼도 None이 아닌지 확인
                latest_date = result[0]
                logger.info(f"Latest daily data date for {stock_code}: {latest_date}")
                return latest_date
            else:
                logger.info(f"No daily data found for {stock_code}.")
                return None
        except Exception as e:
            logger.error(f"Failed to get latest daily data date for {stock_code}: {e}", exc_info=True)
            return None
        finally:
            if conn:
                conn.close()
    
    def save_minute_data(self, data):
        """
        분봉 데이터를 minute_stock_data 테이블에 저장합니다.
        :param data: [{'stock_code': 'A000660', 'datetime': ..., 'open_price': ..., ...}, ...]
        """
        if not data:
            logger.warning("No minute data to save.")
            return 0

        conn = None
        saved_count = 0
        try:
            conn = self.get_db_connection()
            if not conn:
                return 0
            cursor = conn.cursor()

            insert_query = """
            INSERT INTO minute_stock_data (stock_code, datetime, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                open_price=VALUES(open_price),
                high_price=VALUES(high_price),
                low_price=VALUES(low_price),
                close_price=VALUES(close_price),
                volume=VALUES(volume);
            """
            
            records = []
            for record in data:
                records.append((
                    record['stock_code'],
                    record['datetime'], # datetime 객체
                    record['open_price'],
                    record['high_price'],
                    record['low_price'],
                    record['close_price'],
                    record['volume']
                ))
            
            cursor.executemany(insert_query, records)
            conn.commit()
            saved_count = cursor.rowcount
            logger.info(f"Successfully inserted {saved_count} record(s) into minute_stock_data.")
            return saved_count
        except Exception as e:
            logger.error(f"Failed to save minute data: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                conn.close()

    def get_latest_minute_data_datetime(self, stock_code): # interval 인자 제거 (확인)
        """
        특정 종목의 minute_stock_data 테이블에서 가장 최근 날짜/시간을 조회합니다.
        :param stock_code: 조회할 종목 코드
        :return: datetime.datetime 객체 또는 None
        """
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return None
            cursor = conn.cursor()
            # 쿼리에서 `interval` 조건 제거 (확인)
            query = "SELECT MAX(datetime) FROM minute_stock_data WHERE stock_code = %s"
            cursor.execute(query, (stock_code,))
            result = cursor.fetchone() # 결과를 먼저 변수에 할당
            if result is not None and result[0] is not None: # 결과가 None이 아니고, 첫 번째 컬럼도 None이 아닌지 확인
                latest_datetime = result[0]
                logger.info(f"Latest minute data datetime for {stock_code}: {latest_datetime}")
                return latest_datetime
            else:
                logger.info(f"No minute data found for {stock_code}.")
                return None
        except Exception as e:
            logger.error(f"Failed to get latest minute data datetime for {stock_code}: {e}", exc_info=True)
            return None
        finally:
            if conn:
                conn.close()
