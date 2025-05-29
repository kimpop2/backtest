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


# 테스트를 위한 메인 실행 블록 (선택 사항, 개발 중 확인용)
if __name__ == "__main__":
    db_manager = DBManager()

    # 1. DB 연결 테스트
    try:
        conn_test = db_manager.get_db_connection()
        if conn_test:
            print("DB 연결 테스트 성공!")
            conn_test.close()
        else:
            print("DB 연결 테스트 실패!")
    except Exception as e:
        print(f"DB 연결 중 오류 발생: {e}")

    # 2. stock_info 테이블에 데이터 삽입 테스트
    print("\n--- Stock Info Insert Test ---")
    sample_stock_info = [
        {'stock_code': 'A005930', 'stock_name': '삼성전자', 'market_type': 'KOSPI', 'sector': '반도체', 'per': 15.0, 'pbr': 1.5, 'eps': 5000.0},
        {'stock_code': 'A000660', 'stock_name': 'SK하이닉스', 'market_type': 'KOSPI', 'sector': '반도체', 'per': 10.0, 'pbr': 1.2, 'eps': 8000.0}
    ]
    try:
        db_manager.save_stock_info(sample_stock_info)
    except Exception as e:
        print(f"Stock info 삽입 오류: {e}")

    # 3. daily_stock_data 테이블에 데이터 삽입 테스트
    print("\n--- Daily Data Insert Test ---")
    sample_daily_data = [
        {'stock_code': 'A005930', 'date': '2023-01-02', 'open_price': 60000, 'high_price': 61000, 'low_price': 59500, 'close_price': 60500, 'volume': 10000000, 'change_rate': 0.8, 'trading_value': 605000000000},
        {'stock_code': 'A005930', 'date': '2023-01-03', 'open_price': 60500, 'high_price': 61500, 'low_price': 60000, 'close_price': 61000, 'volume': 12000000, 'change_rate': 0.83, 'trading_value': 732000000000},
        {'stock_code': 'A000660', 'date': '2023-01-02', 'open_price': 90000, 'high_price': 91000, 'low_price': 89500, 'close_price': 90500, 'volume': 5000000, 'change_rate': 0.5, 'trading_value': 452500000000}
    ]
    try:
        db_manager.save_daily_data(sample_daily_data)
    except Exception as e:
        print(f"Daily data 삽입 오류: {e}")


    # 4. 데이터 조회 테스트
    print("\n--- Data Fetch Test ---")
    try:
        fetched_info = db_manager.fetch_stock_info(stock_codes=['A005930'])
        print("\nFetched Stock Info (A005930):")
        print(fetched_info)

        fetched_daily = db_manager.fetch_daily_data('A005930', start_date='2023-01-01', end_date='2023-01-03')
        print("\nFetched Daily Data (A005930, 2023-01-01 to 2023-01-03):")
        print(fetched_daily)
    except Exception as e:
        print(f"데이터 조회 오류: {e}")