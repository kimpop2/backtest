# backtest/test.py
import logging
import sys
import os
from datetime import datetime, date, timedelta

# 프로젝트 루트 디렉토리를 sys.path에 추가 (이 부분이 중요합니다)
# test.py가 백테스트 폴더(프로젝트 루트)에 있으므로,
# 이 코드를 통해 config, db 등의 패키지를 인식할 수 있도록 합니다.
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# 이제 db.db_manager를 임포트할 수 있습니다.
from db.db_manager import DBManager #task3
from api_client.creon_api import CreonAPIClient #task4
from data_manager.stock_data_manager import StockDataManager

# 백테스팅 관련 모듈 임포트
from backtester.backtester import Backtester
from strategy.moving_average_crossover import MovingAverageCrossoverStrategy
from db.db_manager import DBManager # DB 초기화 및 데이터 확인용

# 로깅 설정
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler(sys.stdout)
                    ])
logger = logging.getLogger(__name__)

def run_db_tests():
    """DBManager 클래스의 기본 기능을 테스트합니다."""
    print("--- DBManager 통합 테스트 시작 ---")
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
        return # 연결 실패 시 다음 테스트 진행하지 않음

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

    print("\n--- DBManager 통합 테스트 종료 ---")

def run_creon_api_tests():
    """CreonAPIClient 클래스의 기본 기능을 테스트합니다."""
    print("\n--- Creon API Client 통합 테스트 시작 ---")
    creon_api = None
    try:
        creon_api = CreonAPIClient()
        if not creon_api.connected:
            print("Creon Plus 연결에 실패했습니다. 테스트를 건너뜁니다.")
            return

        # 1. 필터링된 모든 종목 정보 가져오기 테스트 (내부 딕셔너리 사용)
        print("\n--- Get Filtered Stock Codes Test ---")
        filtered_codes = creon_api.get_filtered_stock_list()
        if filtered_codes:
            print(f"총 {len(filtered_codes)}개의 필터링된 종목 코드를 가져왔습니다. 예시:")
            sample_stocks = [
                {'code': code, 'name': creon_api.get_stock_name(code)}
                for code in filtered_codes[:5]
            ]
            print(sample_stocks)
        else:
            print("필터링된 종목 정보를 가져오지 못했습니다. HTS 로그인 상태 및 필터링 조건을 확인하세요.")
        
        # 종목명으로 코드 찾기 예시
        print("\n--- Get Stock Code by Name Test ---")
        samsung_code = creon_api.get_stock_code('삼성전자')
        if samsung_code:
            print(f"삼성전자의 종목 코드: {samsung_code}")
        else:
            print("삼성전자 종목 코드를 찾을 수 없습니다.")

        # 2. 특정 종목의 일봉 데이터 가져오기 테스트 (예: 삼성전자 A005930)
        print("\n--- Get Daily OHLCV Test (A005930) ---")
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=100)).strftime('%Y%m%d')
        daily_data_df = creon_api.get_daily_ohlcv('A005930', start_date, end_date)
        if not daily_data_df.empty:
            print(f"삼성전자({daily_data_df['stock_code'].iloc[0]}) 일봉 데이터 {len(daily_data_df)}개를 가져왔습니다. 예시:")
            #print(daily_data_df.head())
            print(daily_data_df)
        else:
            print("삼성전자 일봉 데이터를 가져오지 못했습니다.")

        # 3. 특정 종목의 분봉 데이터 가져오기 테스트 (예: SK하이닉스 A000660, 최근 5일치 1분봉)
        print("\n--- Get Minute OHLCV Test (A000660) ---")
        minute_end_date = datetime.now().strftime('%Y%m%d')
        minute_start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d') # 최근 5일치 분봉
        minute_data_df = creon_api.get_minute_ohlcv('A000660', minute_start_date, minute_end_date, interval=1) # 1분봉
        if not minute_data_df.empty:
            print(f"SK하이닉스({minute_data_df['stock_code'].iloc[0]}) 1분봉 데이터 {len(minute_data_df)}개를 가져왔습니다. 예시:")
            #print(minute_data_df.head())
            print(minute_data_df)
        else:
            print("SK하이닉스 분봉 데이터를 가져오지 못했습니다.")

    except ConnectionError as e:
        print(f"Creon API 연결 오류: {e}")
        print("Creon HTS를 실행하고 로그인 상태인지 확인해 주세요.")
    except Exception as e:
        print(f"Creon API 테스트 중 오류 발생: {e}")
    finally:
        print("\n--- Creon API Client 통합 테스트 종료 ---")


def run_stock_data_manager_tests():
    """StockDataManager 클래스의 기본 기능을 테스트합니다."""
    print("\n--- StockDataManager 통합 테스트 시작 ---")
    stock_data_manager = None
    try:
        stock_data_manager = StockDataManager()

        # 1. 종목 정보 초기화/업데이트 테스트
        print("\n--- Initialize Stock Info Test ---")
        # 매번 테스트 실행 시 DB의 stock_info를 강제로 업데이트하도록 설정
        stock_data_manager.initialize_stock_info(force_update=True) 

        # 2. 일봉 데이터 업데이트 테스트 (삼성전자 A005930)
        print("\n--- Update Daily OHLCV Data Test (A005930) ---")
        end_date_daily = datetime.now().date()
        start_date_daily = end_date_daily - timedelta(days=30)
        stock_data_manager.update_daily_ohlcv_data(stock_code='A005930', start_date=start_date_daily, end_date=end_date_daily)
        
        # 3. 분봉 데이터 업데이트 테스트 (SK하이닉스 A000660, 1분봉, 최근 1일)
        print("\n--- Update Minute OHLCV Data Test (A000660, 1-min) ---")
        minute_end_date = datetime.now().date()
        minute_start_date = minute_end_date # 오늘 하루치 분봉만
        stock_data_manager.update_minute_ohlcv_data(stock_code='A000660', start_date=minute_start_date, end_date=minute_end_date)

        # 4. 데이터 조회 확인 (선택 사항, DBManager 직접 사용)
        print("\n--- Verify Data in DB (Manual Check) ---")
        db_manager = stock_data_manager.db_manager
        fetched_daily = db_manager.fetch_daily_data('A005930', start_date_daily, end_date_daily)
        print(f"\nFetched {len(fetched_daily)} daily records for A005930 from DB.")
        if not fetched_daily.empty:
            print(fetched_daily.tail())

    except ConnectionError as e:
        print(f"StockDataManager 테스트 중 연결 오류: {e}")
    except Exception as e:
        # exc_info=True 인자 제거
        print(f"StockDataManager 테스트 중 오류 발생: {e}") 
    finally:
        print("\n--- StockDataManager 통합 테스트 종료 ---")


def run_full_backtest_test():
    logger.info("--- StockDataManager 통합 테스트 시작 ---")
    # 이전 테스트에서 주석 처리되었던 부분은 여기에 포함되지 않습니다.
    # 만약 StockDataManager 기능 테스트가 필요하다면 별도 함수로 분리하거나,
    # 아래 Backtester.load_data_for_backtest가 그 역할을 대신합니다.
    logger.info("--- StockDataManager 통합 테스트 종료 ---")

    logger.info("--- Backtester 통합 테스트 시작 ---")

    # 1. DBManager를 사용하여 DB 초기화 (선택 사항: 이전 테스트에서 데이터가 쌓여있다면 스킵 가능)
    # 깨끗한 상태에서 시작하고 싶다면 주석 해제하여 실행
    db_manager = DBManager()
    db_manager.drop_all_tables()
    db_manager.create_all_tables()
    logger.info("DB tables dropped and recreated for clean test.")

    # 2. 백테스팅 전략 인스턴스 생성
    # 단기 5일, 장기 20일 이동평균선 전략
    strategy = MovingAverageCrossoverStrategy(short_window=2, long_window=7)

    # 3. 백테스터 인스턴스 생성
    backtester = Backtester(
        strategy=strategy,
        initial_capital=1_000_000, # 1억 원
        commission_rate=0.00015,
        slippage_rate=0.0001
    )

    # 4. 백테스팅 대상 종목 및 기간 설정
    # 테스트를 위해 실제 데이터가 있는 종목 (삼성전자, SK하이닉스 등)을 선택하는 것이 좋습니다.
    # Creon API는 보통 당일 포함 최근 600일 정도의 일봉 데이터를 제공합니다.
    # 테스트 기간은 너무 길지 않게 설정하여 데이터 로딩 시간을 줄입니다.
    #test_stocks = ['A005930', 'A000660', 'A035720'] # 삼성전자, SK하이닉스, 카카오
    test_stocks = ['A012450', 'A042660', 'A103140'] # 한화에어로스페이스, 한화오션, 풍산
    test_start_date = date(2024, 1, 2) # 데이터가 존재하는 적절한 시작 날짜
    test_end_date = date(2025, 5, 28) # 오늘 날짜에 너무 가까우면 당일 데이터가 없을 수 있으므로, 며칠 전으로 설정

    # 5. 백테스팅 데이터 로드 (DB에 데이터가 없으면 Creon API에서 가져와 DB에 저장)
    logger.info("Step 5: Loading data for backtest...")
    # 분봉 테스트 시 True로 변경
    backtester.load_data_for_backtest(test_stocks, test_start_date, test_end_date, is_minute_data=False)
    logger.info("Step 5: Data loading complete.")

    # 6. 백테스팅 실행
    logger.info("Step 6: Running backtest...")
    final_results, trade_logs, portfolio_history_df = backtester.run_backtest()
    logger.info("Step 6: Backtest execution complete.")

    # 7. 백테스팅 결과 출력 (DB에 저장된 내용 확인)
    logger.info("\n--- Backtest Final Results ---")
    for key, value in final_results.items():
        if isinstance(value, float):
            logger.info(f"{key}: {value:.2f}")
        else:
            logger.info(f"{key}: {value}")

    logger.info("\n--- Last 5 Trade Logs (from PortfolioManager) ---")
    if trade_logs:
        for log in trade_logs[-5:]: # 마지막 5개 로그 출력
            logger.info(log)
    else:
        logger.info("No trade logs generated.")

    logger.info("\n--- Last 5 Portfolio Value History (from PortfolioManager) ---")
    if not portfolio_history_df.empty:
        logger.info(portfolio_history_df.tail())
    else:
        logger.info("No portfolio value history generated.")
        
    logger.info("\n--- Verify Data in DB (Manual Check Required) ---")
    logger.info("Please check 'backtest_results' and 'trade_log' tables in your MariaDB to confirm data storage.")

    logger.info("--- Backtester 통합 테스트 종료 ---")

if __name__ == "__main__":
    #run_db_tests()
    
    #run_creon_api_tests()
    #run_stock_data_manager_tests()
    run_full_backtest_test()