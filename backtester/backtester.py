# backtest/backtester/backtester.py

import logging
from datetime import datetime, date, timedelta
import pandas as pd
import sys
import os
from typing import Dict, List 

# 프로젝트 루트 경로를 sys.path에 추가하여 모듈 임포트 가능하게 함
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db.db_manager import DBManager
from data_manager.stock_data_manager import StockDataManager
from strategy.base_strategy import BaseStrategy
from backtester.portfolio_manager import PortfolioManager

logger = logging.getLogger(__name__)

class Backtester:
    def __init__(self,
                 strategy: BaseStrategy,
                 initial_capital: float = 100_000_000, # 1억 원
                 commission_rate: float = 0.00015, # 0.015% (매수/매도 각각)
                 slippage_rate: float = 0.0001 # 0.01%
                ):
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

        self.db_manager = DBManager()
        self.stock_data_manager = StockDataManager()
        self.portfolio_manager = PortfolioManager(
            initial_capital=self.initial_capital,
            commission_rate=self.commission_rate,
            slippage_rate=self.slippage_rate
        )
        self.stock_list = [] # 백테스팅 대상 종목 리스트
        self.start_date = None
        self.end_date = None
        self.is_minute_data_test = False # 분봉 데이터로 테스트할 것인지 여부

        logger.info(f"Backtester initialized with strategy: {self.strategy.get_name()}")
        logger.info(f"Initial Capital: {self.initial_capital}, Commission: {self.commission_rate}, Slippage: {self.slippage_rate}")

    def load_data_for_backtest(self, stock_list: List[str], start_date: date, end_date: date, is_minute_data: bool):
        """
        백테스팅에 필요한 데이터를 DB에 로드하고, 필요한 경우 Creon API에서 가져옵니다.
        """
        self.stock_list = stock_list
        self.start_date = start_date
        self.end_date = end_date
        self.is_minute_data_test = is_minute_data

        logger.info(f"Loading data for {len(stock_list)} stocks from {start_date} to {end_date}...")
        
        # stock_info가 없는 경우 먼저 초기화
        if self.db_manager.get_stock_info_count() == 0:
             logger.info("No stock info found in DB. Initializing stock info...")
             self.stock_data_manager.initialize_stock_info(force_update=True) # 강제 업데이트 (처음 실행 시)

        # 백테스트 대상 종목들의 데이터만 가져옵니다.
        for stock_code in stock_list:
            if self.is_minute_data_test:
                # 분봉 데이터는 일별로 조회하는 대신, 해당 날짜의 모든 분봉 데이터를 가져와서 내부적으로 분 단위로 순회해야 함.
                # 여기서는 일단 일봉처럼 '해당 날짜의 데이터'만 가져오도록 추상화. 실제 분봉은 아래 별도 로직.
                # 분봉 백테스트 로직은 일봉 백테스트와 분리해서 구현하는 것이 합리적임.
                pass # 아래 else if 분봉 처리 로직으로 대체될 것임
            else:
                self.stock_data_manager.update_daily_ohlcv_data(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date
                )
        logger.info("All required data loaded into DB.")

    def run_backtest(self):
        """
        백테스팅을 실행합니다.
        """
        if not self.stock_list or not self.start_date or not self.end_date:
            logger.error("Backtest data not loaded. Please call load_data_for_backtest first.")
            return

        logger.info(f"Starting backtest from {self.start_date} to {self.end_date} for {len(self.stock_list)} stocks.")
        
        # --- 핵심 변경 사항: strategy.on_init 호출 시 portfolio_manager 전달 ---
        self.strategy.on_init(self.initial_capital, self.stock_list, self.portfolio_manager)
        # ----------------------------------------------------------------------

        current_date_iter = self.start_date
        while current_date_iter <= self.end_date:
            logger.info(f"Processing date: {current_date_iter}")

            # 1. 해당 날짜의 모든 종목 데이터 로드
            all_data_for_day = {}
            market_prices_for_day = {} # 포트폴리오 매니저 업데이트용

            for stock_code in self.stock_list:
                if self.is_minute_data_test:
                    # 분봉 데이터는 일별로 조회하는 대신, 해당 날짜의 모든 분봉 데이터를 가져와서 내부적으로 분 단위로 순회해야 함.
                    # 여기서는 일단 일봉처럼 '해당 날짜의 데이터'만 가져오도록 추상화. 실제 분봉은 아래 별도 로직.
                    # 분봉 백테스트 로직은 일봉 백테스트와 분리해서 구현하는 것이 합리적임.
                    pass # 아래 else if 분봉 처리 로직으로 대체될 것임
                else:
                    # 일봉 데이터 조회 (해당 날짜까지의 모든 데이터)
                    daily_df = self.db_manager.get_daily_data(stock_code, self.start_date, current_date_iter)
                    if not daily_df.empty:
                        all_data_for_day[stock_code] = daily_df
                        # 현재 날짜의 종가를 PortfolioManager에 전달
                        market_prices_for_day[stock_code] = daily_df.iloc[-1]['close_price'] # 마지막 행의 종가
                    else:
                        logger.warning(f"No daily data for {stock_code} up to {current_date_iter}. Skipping for this date.")

            # 2. PortfolioManager 업데이트 (시장가치 반영)
            self.portfolio_manager.update_current_market_data(current_date_iter, market_prices_for_day)

            # 3. 전략에 일별 데이터 전달 및 신호 생성 요청
            if not self.is_minute_data_test:
                # 일봉 백테스트 로직
                if all_data_for_day: # 적어도 하나의 종목 데이터가 있다면
                    signals = self.strategy.on_daily_data(current_date_iter, all_data_for_day)
                    if signals:
                        logger.info(f"[{current_date_iter}] Received {len(signals)} signal(s).")
                        for signal in signals:
                            self.portfolio_manager.execute_order(signal)
            else:
                # 분봉 백테스트 로직 (일별 루프 내에서 시간 단위로 다시 루프)
                # 해당 날짜의 모든 분봉 데이터 로드 (모든 종목)
                all_minute_data_for_day_raw = {}
                for stock_code in self.stock_list:
                    # 특정 날짜의 모든 분봉 데이터를 가져오기 (db_manager에 이 메서드 필요)
                    minute_df = self.db_manager.get_minute_data_for_date(stock_code, current_date_iter)
                    if not minute_df.empty:
                        all_minute_data_for_day_raw[stock_code] = minute_df

                if all_minute_data_for_day_raw:
                    # 해당 날짜의 분봉 데이터 시간대 정렬 및 순회
                    # 모든 종목의 분봉 데이터를 합쳐서 시간 순서로 정렬하고 각 시간대별로 처리
                    # 예: 삼성전자 09:00, SK하이닉스 09:00, ... -> 처리 -> 삼성전자 09:01, SK하이닉스 09:01 ...
                    all_datetimes = sorted(
                        list(set(dt for df in all_minute_data_for_day_raw.values() for dt in df.index))
                    )
                    
                    for current_datetime_in_minute in all_datetimes:
                        # 해당 시간까지의 모든 종목 데이터 구성
                        current_all_minute_data = {}
                        current_market_prices_minute = {}
                        for stock_code, df in all_minute_data_for_day_raw.items():
                            # 현재 시간까지의 데이터만 슬라이싱
                            # df.index는 datetime 타입, 현재 시간까지 포함
                            filtered_df = df[df.index <= current_datetime_in_minute] 
                            if not filtered_df.empty:
                                current_all_minute_data[stock_code] = filtered_df
                                current_market_prices_minute[stock_code] = filtered_df.iloc[-1]['close_price']
                        
                        if current_all_minute_data:
                            # PortfolioManager 업데이트 (시장가치 반영 - 분봉 기준)
                            self.portfolio_manager.update_current_market_data(current_datetime_in_minute, current_market_prices_minute)
                            
                            # 전략에 분별 데이터 전달 및 신호 생성 요청
                            signals_minute = self.strategy.on_minute_data(current_datetime_in_minute, current_all_minute_data)
                            if signals_minute:
                                logger.info(f"[{current_datetime_in_minute}] Received {len(signals_minute)} minute signal(s).")
                                for signal in signals_minute:
                                    self.portfolio_manager.execute_order(signal)
            
            current_date_iter += timedelta(days=1)

        self.strategy.on_finish()
        logger.info("Backtest finished.")
        
        # 최종 결과 저장 및 반환
        final_results = self.portfolio_manager.get_final_results()
        trade_logs = self.portfolio_manager.get_trade_logs()
        portfolio_history = self.portfolio_manager.get_portfolio_value_history()

        # DB에 결과 저장 (backtest_results, trade_log 테이블)
        self._save_backtest_results_to_db(final_results, trade_logs)

        return final_results, trade_logs, portfolio_history

    def _save_backtest_results_to_db(self, final_results: dict, trade_logs: List[dict]):
        """
        백테스팅 최종 결과와 거래 로그를 DB에 저장합니다.
        """
        logger.info("Saving backtest results to DB...")
        conn = None
        try:
            conn = self.db_manager.get_db_connection()
            if not conn:
                logger.error("Failed to connect to DB for saving results.")
                return

            cursor = conn.cursor()

            # 1. backtest_results 테이블에 최종 결과 저장
            insert_result_query = """
            INSERT INTO backtest_results (
                strategy_name, start_date, end_date, initial_capital, final_capital,
                total_return, annualized_return, max_drawdown, sharpe_ratio,
                total_trades, win_rate, profit_factor, commission_rate, slippage_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            # TODO: annualized_return, sharpe_ratio, profit_factor는 PerformanceAnalyzer에서 계산 후 여기에 추가
            # 지금은 임시로 0.0 또는 N/A로 채움
            result_data = (
                self.strategy.get_name(),
                self.start_date,
                self.end_date,
                final_results['initial_capital'],
                final_results['final_capital'],
                final_results['total_return'],
                0.0, # annualized_return (TODO: PerformanceAnalyzer)
                final_results['max_drawdown'],
                0.0, # sharpe_ratio (TODO: PerformanceAnalyzer)
                final_results['total_trades'],
                final_results['win_rate'],
                0.0, # profit_factor (TODO: PerformanceAnalyzer)
                final_results['commission_rate'],
                final_results['slippage_rate']
            )
            cursor.execute(insert_result_query, result_data)
            result_id = cursor.lastrowid # 삽입된 백테스트 결과의 ID
            logger.info(f"Backtest result saved with ID: {result_id}")

            # 2. trade_log 테이블에 상세 거래 내역 저장
            if trade_logs:
                insert_trade_query = """
                INSERT INTO trade_log (
                    result_id, stock_code, trade_date, trade_type, price, quantity,
                    commission, slippage, pnl, position_size, portfolio_value
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                trade_records = []
                for log in trade_logs:
                    trade_records.append((
                        result_id,
                        log['stock_code'],
                        log['trade_date'],
                        log['trade_type'],
                        log['price'],
                        log['quantity'],
                        log['commission'],
                        log['slippage'],
                        log['pnl'],
                        log['position_size'],
                        log['portfolio_value']
                    ))
                cursor.executemany(insert_trade_query, trade_records)
                logger.info(f"Saved {len(trade_records)} trade logs for result ID: {result_id}")
            else:
                logger.info("No trade logs to save.")

            conn.commit()

        except Exception as e:
            logger.error(f"Failed to save backtest results to DB: {e}", exc_info=True)
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()