# backtest/strategy/moving_average_crossover.py

import pandas as pd
from datetime import datetime, date
import logging
from strategy.base_strategy import BaseStrategy
from typing import Dict, List 
# PortfolioManager 타입을 명시적으로 임포트하여 타입 힌팅에 사용
from backtester.portfolio_manager import PortfolioManager 

logger = logging.getLogger(__name__)

class MovingAverageCrossoverStrategy(BaseStrategy):
    """
    이동평균선(MA) 크로스오버 전략입니다.
    단기 이동평균선이 장기 이동평균선을 상향 돌파하면 매수, 하향 돌파하면 매도합니다.
    """
    def __init__(self, short_window: int = 5, long_window: int = 20, **kwargs):
        super().__init__("MovingAverageCrossover", **kwargs)
        self.short_window = short_window
        self.long_window = long_window
        self.params.update({
            'short_window': self.short_window,
            'long_window': self.long_window
        })
        # 각 종목별로 이전 MA 값을 저장하여 크로스오버를 감지
        self.previous_short_ma = {} # {'stock_code': value}
        self.previous_long_ma = {}  # {'stock_code': value}
        # self.current_positions는 더 이상 전략에서 직접 관리하지 않습니다.

        # 백테스터로부터 PortfolioManager 인스턴스를 전달받을 예정
        self.portfolio_manager: PortfolioManager = None 

        logger.info(f"Initialized MovingAverageCrossoverStrategy with short_window={self.short_window}, long_window={self.long_window}")

    def on_init(self, initial_capital: float, stock_list: list, portfolio_manager: PortfolioManager):
        """
        전략 초기화. 백테스트 대상 종목별 초기 상태 설정.
        :param initial_capital: 초기 자본
        :param stock_list: 백테스트 대상 종목 리스트
        :param portfolio_manager: PortfolioManager 인스턴스 (Backtester로부터 전달받음)
        """
        self.logger.info(f"Strategy initialized with initial capital: {initial_capital}, stocks: {stock_list}")
        self.portfolio_manager = portfolio_manager # PortfolioManager 인스턴스 저장

        for stock_code in stock_list:
            self.previous_short_ma[stock_code] = None
            self.previous_long_ma[stock_code] = None
            # self.current_positions 초기화는 더 이상 필요 없습니다.

    def generate_signal(self, stock_code: str, current_data: pd.DataFrame) -> dict:
        """
        주어진 종목의 데이터를 기반으로 매매 신호를 생성합니다.
        
        :param stock_code: 현재 신호를 생성할 종목 코드.
        :param current_data: 해당 stock_code에 대한 현재 시점까지의 OHLCV 데이터 (pandas DataFrame).
                              인덱스는 datetime/date, 컬럼은 'open_price', 'high_price', 'low_price', 'close_price', 'volume' 등을 포함합니다.
        :return: 매매 신호를 담은 딕셔너리.
        """
        if self.portfolio_manager is None:
            self.logger.error("PortfolioManager not set in strategy. Cannot generate signals.")
            return {'signal': 'HOLD'}

        if current_data.empty or len(current_data) < max(self.short_window, self.long_window):
            # 이동평균 계산에 필요한 최소 데이터가 없는 경우
            return {'signal': 'HOLD'}

        # 최신 데이터를 기준으로 이동평균 계산
        if 'close_price' not in current_data.columns:
            self.logger.warning(f"[{stock_code}] 'close_price' column not found in data. Cannot calculate MA.")
            return {'signal': 'HOLD'}

        short_ma = current_data['close_price'].iloc[-self.short_window:].mean()
        long_ma = current_data['close_price'].iloc[-self.long_window:].mean()
        
        signal = {'signal': 'HOLD', 'stock_code': stock_code}
        current_price = current_data['close_price'].iloc[-1]
        current_time = current_data.index[-1] # 날짜 또는 날짜/시간

        # PortfolioManager를 통해 현재 보유 수량 조회
        current_holding_quantity = self.portfolio_manager.get_holding_quantity(stock_code)

        if self.previous_short_ma[stock_code] is not None and self.previous_long_ma[stock_code] is not None:
            # 매수 조건: 단기 MA가 장기 MA를 상향 돌파 (골든 크로스)
            if (self.previous_short_ma[stock_code] <= self.previous_long_ma[stock_code]) and (short_ma > long_ma):
                if current_holding_quantity == 0: # 현재 보유하고 있지 않을 때만 매수
                    signal['signal'] = 'BUY'
                    signal['price'] = current_price # 현재 종가로 매수
                    signal['quantity'] = 10 # 예시: 10주 매수 (실제는 자본금에 따라 유동적으로 결정)
                    self.logger.info(f"[{current_time}] {stock_code} BUY Signal (Golden Cross): Short MA {short_ma:.2f} > Long MA {long_ma:.2f}")
                else:
                    self.logger.debug(f"[{current_time}] {stock_code} BUY signal ignored (already holding {current_holding_quantity} shares).")
            
            # 매도 조건: 단기 MA가 장기 MA를 하향 돌파 (데드 크로스)
            elif (self.previous_short_ma[stock_code] >= self.previous_long_ma[stock_code]) and (short_ma < long_ma):
                if current_holding_quantity > 0: # 현재 보유하고 있을 때만 매도
                    signal['signal'] = 'SELL'
                    signal['price'] = current_price # 현재 종가로 매도
                    signal['quantity'] = current_holding_quantity # 보유한 모든 수량 매도
                    self.logger.info(f"[{current_time}] {stock_code} SELL Signal (Dead Cross): Short MA {short_ma:.2f} < Long MA {long_ma:.2f}")
                else:
                    self.logger.debug(f"[{current_time}] {stock_code} SELL signal ignored (no position to sell).")

        # 현재 MA 값 저장
        self.previous_short_ma[stock_code] = short_ma
        self.previous_long_ma[stock_code] = long_ma
        
        return signal

    def on_daily_data(self, current_date: date, all_daily_data: Dict[str, pd.DataFrame]) -> List[dict]:
        signals = []
        for stock_code, data_df in all_daily_data.items():
            signal = self.generate_signal(stock_code, data_df)
            if signal and signal['signal'] != 'HOLD':
                signals.append(signal)
        return signals

    def on_minute_data(self, current_datetime: datetime, all_minute_data: Dict[str, pd.DataFrame]) -> List[dict]:
        signals = []
        for stock_code, data_df in all_minute_data.items():
            signal = self.generate_signal(stock_code, data_df)
            if signal and signal['signal'] != 'HOLD':
                signals.append(signal)
        return signals

    def on_finish(self):
        self.logger.info("MovingAverageCrossoverStrategy finished.")