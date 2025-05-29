# backtest/test.py

import sys
import os
from datetime import datetime

# 프로젝트 루트 디렉토리를 sys.path에 추가 (이 부분이 중요합니다)
# test.py가 백테스트 폴더(프로젝트 루트)에 있으므로,
# 이 코드를 통해 config, db 등의 패키지를 인식할 수 있도록 합니다.
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# 이제 db.db_manager를 임포트할 수 있습니다.
from db.db_manager import DBManager

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


if __name__ == "__main__":
    run_db_tests()