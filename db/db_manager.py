# backtest/db/db_manager.py

import pymysql
from dotenv import load_dotenv
import os
from config.settings import DB_HOST, DB_PORT, DB_NAME
import logging
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Any

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
    def save_stock_info(self, stock_info_list: List[Dict[str, Any]]): # 'Any'를 위해 from typing import Any 추가 필요
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                logger.error("Failed to connect to DB for saving stock info.")
                return False
            cursor = conn.cursor()

            # ON DUPLICATE KEY UPDATE 절을 사용하여 중복 시 업데이트하도록 변경
            query = """
            INSERT INTO stock_info (stock_code, stock_name, market_type, sector, per, pbr, eps)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                market_type = VALUES(market_type),
                sector = VALUES(sector),
                per = VALUES(per),
                pbr = VALUES(pbr),
                eps = VALUES(eps);
            """
            
            records = []
            for item in stock_info_list:
                records.append((
                    item.get('stock_code'), item.get('stock_name'), item.get('market_type'),
                    item.get('sector'), item.get('per'), item.get('pbr'), item.get('eps')
                ))
            
            cursor.executemany(query, records)
            conn.commit()
            logger.info(f"Saved {len(stock_info_list)} stock info records to DB.")
            return True
        except Exception as e:
            logger.error(f"Error inserting data into stock_info. Error: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

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


    def get_stock_info_count(self) -> int:
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                logger.error("Failed to connect to DB for get_stock_info_count.")
                return 0
            cursor = conn.cursor()
            query = "SELECT COUNT(*) AS count FROM stock_info;" # AS count 추가
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                count = result['count'] # result[0] 대신 result['count'] 사용
                return count
            return 0
        except Exception as e:
            logger.error(f"Failed to get stock info count: {e}", exc_info=True)
            return 0
        finally:
            if conn:
                conn.close()


    def get_latest_daily_data_date(self, stock_code: str) -> date:
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return date(1900, 1, 1) # 연결 실패 시 기본값 반환

            cursor = conn.cursor()
            # `datetime` 컬럼이 아니므로 `DATE_FORMAT` 필요 없음, `MAX(date)` 사용
            query = """
            SELECT MAX(date) AS latest_date
            FROM daily_stock_data
            WHERE stock_code = %s;
            """
            cursor.execute(query, (stock_code,))
            result = cursor.fetchone()

            # result['latest_date']로 접근
            if result and result['latest_date'] is not None:
                return result['latest_date']
            else:
                return date(1900, 1, 1) # 데이터가 없는 경우 (가장 오래된 날짜)
        except Exception as e:
            logger.error(f"Failed to get latest daily data date for {stock_code}: {e}", exc_info=True)
            return date(1900, 1, 1)
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

    def get_latest_minute_data_datetime(self, stock_code: str) -> datetime:
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return datetime(1900, 1, 1, 0, 0, 0)

            cursor = conn.cursor()
            query = """
            SELECT MAX(datetime) AS latest_datetime
            FROM minute_stock_data
            WHERE stock_code = %s;
            """
            cursor.execute(query, (stock_code,))
            result = cursor.fetchone()

            if result and result['latest_datetime'] is not None:
                return result['latest_datetime']
            else:
                return datetime(1900, 1, 1, 0, 0, 0)
        except Exception as e:
            logger.error(f"Failed to get latest minute data datetime for {stock_code}: {e}", exc_info=True)
            return datetime(1900, 1, 1, 0, 0, 0)
        finally:
            if conn:
                conn.close()

    def get_daily_data(self, stock_code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """
        특정 종목의 일봉 데이터를 지정된 기간 동안 조회합니다.
        :param stock_code: 종목 코드
        :param start_date: 시작 날짜 (inclusive)
        :param end_date: 종료 날짜 (inclusive)
        :return: Pandas DataFrame
        """
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return pd.DataFrame()
            query = """
            SELECT stock_code, date, open_price, high_price, low_price, close_price, volume, change_rate, trading_value
            FROM daily_stock_data
            WHERE stock_code = %s AND date BETWEEN %s AND %s
            ORDER BY date ASC;
            """
            df = pd.read_sql(query, conn, params=(stock_code, start_date, end_date), index_col='date')
            return df
        except Exception as e:
            logger.error(f"Failed to get daily data for {stock_code}: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()

    def get_minute_data_for_date(self, stock_code: str, target_date: date) -> pd.DataFrame:
        """
        특정 종목의 특정 날짜에 해당하는 모든 분봉 데이터를 조회합니다.
        :param stock_code: 종목 코드
        :param target_date: 조회할 날짜
        :return: Pandas DataFrame
        """
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return pd.DataFrame()
            query = """
            SELECT stock_code, datetime, open_price, high_price, low_price, close_price, volume
            FROM minute_stock_data
            WHERE stock_code = %s AND DATE(datetime) = %s
            ORDER BY datetime ASC;
            """
            # datetime 컬럼이 인덱스가 되어야 하므로, parse_dates와 index_col을 사용
            df = pd.read_sql(query, conn, params=(stock_code, target_date), parse_dates=['datetime'], index_col='datetime')
            return df
        except Exception as e:
            logger.error(f"Failed to get minute data for {stock_code} on {target_date}: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()


    def create_all_tables(self):
        """필요한 모든 테이블을 생성합니다."""
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                logger.error("Failed to connect to DB for table creation.")
                return False
            
            cursor = conn.cursor()

            # stock_info 테이블 (종목 기본 정보)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_info (
                stock_code VARCHAR(10) PRIMARY KEY,
                stock_name VARCHAR(100) NOT NULL,
                market_type VARCHAR(20),
                sector VARCHAR(100),
                per DECIMAL(10, 2),
                pbr DECIMAL(10, 2),
                eps DECIMAL(15, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("Table 'stock_info' ensured.")

            # daily_stock_data 테이블 (일별 주가 데이터)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_stock_data (
                stock_code VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open_price DECIMAL(15, 2) NOT NULL,
                high_price DECIMAL(15, 2) NOT NULL,
                low_price DECIMAL(15, 2) NOT NULL,
                close_price DECIMAL(15, 2) NOT NULL,
                volume BIGINT NOT NULL,
                change_rate DECIMAL(10, 4),
                trading_value BIGINT,
                PRIMARY KEY (stock_code, date)
            ) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("Table 'daily_stock_data' ensured.")

            # minute_stock_data 테이블 (분별 주가 데이터)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS minute_stock_data (
                stock_code VARCHAR(10) NOT NULL,
                datetime DATETIME NOT NULL,
                open_price DECIMAL(15, 2) NOT NULL,
                high_price DECIMAL(15, 2) NOT NULL,
                low_price DECIMAL(15, 2) NOT NULL,
                close_price DECIMAL(15, 2) NOT NULL,
                volume BIGINT NOT NULL,
                PRIMARY KEY (stock_code, datetime)
            ) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("Table 'minute_stock_data' ensured.")
            
            # backtest_results 테이블 (백테스팅 결과 요약)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS backtest_results (
                result_id INT AUTO_INCREMENT PRIMARY KEY,
                strategy_name VARCHAR(100) NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                initial_capital DECIMAL(20, 2) NOT NULL,
                final_capital DECIMAL(20, 2) NOT NULL,
                total_return DECIMAL(10, 2),
                annualized_return DECIMAL(10, 2),
                max_drawdown DECIMAL(10, 2),
                sharpe_ratio DECIMAL(10, 4),
                total_trades INT,
                win_rate DECIMAL(10, 2),
                profit_factor DECIMAL(10, 2),
                commission_rate DECIMAL(10, 5),
                slippage_rate DECIMAL(10, 5),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("Table 'backtest_results' ensured.")

            # trade_log 테이블 (개별 거래 내역)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_log (
                trade_id INT AUTO_INCREMENT PRIMARY KEY,
                result_id INT NOT NULL,
                stock_code VARCHAR(10) NOT NULL,
                trade_date DATETIME NOT NULL,
                trade_type VARCHAR(10) NOT NULL, -- 'BUY' or 'SELL'
                price DECIMAL(15, 2) NOT NULL,
                quantity INT NOT NULL,
                commission DECIMAL(15, 2) NOT NULL,
                slippage DECIMAL(15, 2) NOT NULL,
                pnl DECIMAL(15, 2), -- Profit and Loss for this trade (realized)
                position_size INT, -- position after this trade
                portfolio_value DECIMAL(20, 2), -- portfolio value after this trade
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (result_id) REFERENCES backtest_results(result_id) ON DELETE CASCADE
            ) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("Table 'trade_log' ensured.")

            conn.commit()
            logger.info("All tables created successfully (if not already existing).")
            return True
        except pymysql.Error as e:
            logger.error(f"Error creating tables: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def drop_all_tables(self):
        """모든 테이블을 삭제합니다."""
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                logger.error("Failed to connect to DB for table dropping.")
                return False
            
            cursor = conn.cursor()
            
            # 외래 키 제약 조건 비활성화 (테이블 삭제 순서 때문에)
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

            tables = ["trade_log", "backtest_results", "minute_stock_data", "daily_stock_data", "stock_info"]
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table};")
                logger.info(f"Table '{table}' dropped.")
            
            # 외래 키 제약 조건 다시 활성화
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            
            conn.commit()
            logger.info("All tables dropped successfully.")
            return True
        except pymysql.Error as e:
            logger.error(f"Error dropping tables: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()                