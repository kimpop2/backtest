# backtest/strategy/moving_average_crossover.py

import pandas as pd
from datetime import datetime, date
import logging
from strategy.base_strategy import BaseStrategy
from typing import Dict, List 

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
        self.current_positions = {} # {'stock_code': True/False (보유 여부)}

        logger.info(f"Initialized MovingAverageCrossoverStrategy with short_window={self.short_window}, long_window={self.long_window}")

    def on_init(self, initial_capital: float, stock_list: list):
        """
        전략 초기화. 백테스트 대상 종목별 초기 상태 설정.
        """
        self.logger.info(f"Strategy initialized with initial capital: {initial_capital}, stocks: {stock_list}")
        for stock_code in stock_list:
            self.previous_short_ma[stock_code] = None
            self.previous_long_ma[stock_code] = None
            self.current_positions[stock_code] = False # 초기에는 보유하지 않음

    def generate_signal(self, stock_code: str, current_data: pd.DataFrame) -> dict:
        """
        주어진 종목의 데이터를 기반으로 매매 신호를 생성합니다.
        
        :param stock_code: 현재 신호를 생성할 종목 코드.
        :param current_data: 해당 stock_code에 대한 현재 시점까지의 OHLCV 데이터 (pandas DataFrame).
                             인덱스는 datetime/date, 컬럼은 'open_price', 'high_price', 'low_price', 'close_price', 'volume' 등을 포함합니다.
        :return: 매매 신호를 담은 딕셔너리.
        """
        if current_data.empty or len(current_data) < max(self.short_window, self.long_window):
            # 이동평균 계산에 필요한 최소 데이터가 없는 경우
            return {'signal': 'HOLD'}

        # 최신 데이터를 기준으로 이동평균 계산
        # close_price 컬럼이 있는지 확인 (Creon API 데이터와 일치)
        if 'close_price' not in current_data.columns:
            self.logger.warning(f"[{stock_code}] 'close_price' column not found in data. Cannot calculate MA.")
            return {'signal': 'HOLD'}

        short_ma = current_data['close_price'].iloc[-self.short_window:].mean()
        long_ma = current_data['close_price'].iloc[-self.long_window:].mean()
        
        signal = {'signal': 'HOLD', 'stock_code': stock_code}
        current_price = current_data['close_price'].iloc[-1]
        current_time = current_data.index[-1] # 날짜 또는 날짜/시간

        if self.previous_short_ma[stock_code] is not None and self.previous_long_ma[stock_code] is not None:
            # 매수 조건: 단기 MA가 장기 MA를 상향 돌파 (골든 크로스)
            if (self.previous_short_ma[stock_code] <= self.previous_long_ma[stock_code]) and (short_ma > long_ma):
                if not self.current_positions[stock_code]: # 현재 보유하고 있지 않을 때만 매수
                    signal['signal'] = 'BUY'
                    signal['price'] = current_price # 현재 종가로 매수
                    signal['quantity'] = 1 # 일단 1주 매수로 가정 (백테스터에서 수량 관리)
                    self.logger.info(f"[{current_time}] {stock_code} BUY Signal (Golden Cross): Short MA {short_ma:.2f} > Long MA {long_ma:.2f}")
                    # self.current_positions[stock_code] = True # 백테스터에서 실제 거래 성공 시 업데이트됨
            
            # 매도 조건: 단기 MA가 장기 MA를 하향 돌파 (데드 크로스)
            elif (self.previous_short_ma[stock_code] >= self.previous_long_ma[stock_code]) and (short_ma < long_ma):
                if self.current_positions[stock_code]: # 현재 보유하고 있을 때만 매도
                    signal['signal'] = 'SELL'
                    signal['price'] = current_price # 현재 종가로 매도
                    signal['quantity'] = self.current_positions[stock_code] # 실제 보유 수량으로 매도 (백테스터에서 실제 수량 결정)
                                                                         # 여기서는 True/False를 저장했으므로, 실제 수량으로 변경 필요
                                                                         # 일단 백테스터가 포트폴리오 정보를 주지 않으므로, 임시로 최대 수량으로 설정
                                                                         # PortfolioManager에서 보유 수량 조회하여 전달하는 방식으로 변경해야 함
                    signal['quantity'] = 100 # 임시로 100주 매도로 가정. 실제는 백테스터가 보유 수량에 따라 조정
                    self.logger.info(f"[{current_time}] {stock_code} SELL Signal (Dead Cross): Short MA {short_ma:.2f} < Long MA {long_ma:.2f}")
                    # self.current_positions[stock_code] = False

        # 현재 MA 값 저장
        self.previous_short_ma[stock_code] = short_ma
        self.previous_long_ma[stock_code] = long_ma
        
        return signal

    def on_daily_data(self, current_date: date, all_daily_data: Dict[str, pd.DataFrame]) -> List[dict]:
        """
        일별 데이터가 주어졌을 때 모든 종목에 대해 매매 신호를 생성합니다.
        """
        signals = []
        for stock_code, data_df in all_daily_data.items():
            # generate_signal은 해당 종목의 누적 데이터를 받아 신호를 생성합니다.
            # BaseStrategy의 generate_signal 정의에 맞게 current_data를 통째로 전달.
            signal = self.generate_signal(stock_code, data_df)
            if signal and signal['signal'] != 'HOLD':
                signals.append(signal)
            
            # 매매가 일어났을 경우 PortfolioManager에서 current_positions 업데이트 필요.
            # 전략 자체는 포지션 업데이트 정보를 직접 알 수 없음. 백테스터가 매매 성공 후 알려주어야 함.
            # 여기서는 단순히 signal을 반환하고 백테스터가 처리하도록 함.
            
            # 임시 방편: 백테스터가 실제 매매를 성공시킨 후, 전략에 포지션 변화를 알려주는 방법 필요
            # 아니면, 전략이 PortfolioManager의 현재 보유 정보를 조회할 수 있도록 인터페이스 추가
            # 현재는 PortfolioManager가 execute_order 후 self.holdings 업데이트하므로,
            # 전략이 직접 포지션 정보를 유지하기보다, 매매 신호만 생성하도록 초점 맞춤.
            # 이 전략에서는 단지 `self.current_positions[stock_code]`를 사용했는데,
            # 이는 PortfolioManager의 실제 포지션과 동기화되어야 함.
            # 일단 여기서는 Strategy가 매매 신호를 독립적으로 생성하고,
            # PortfolioManager가 매매를 처리한 후 그 결과를 Strategy에 알려주는 구조가 필요.
            # 지금은 간단하게, BUY/SELL 신호가 나갈 때마다 current_positions를 업데이트하는 방식으로 구현. (위 코드에 주석처리)

            # NOTE: 전략이 `self.current_positions`를 직접 관리하는 방식은 백테스터의 포트폴리오와 불일치할 수 있음.
            # 이상적으로는 백테스터가 전략으로부터 신호를 받은 후 매매를 처리하고,
            # 그 결과 (실제 체결된 포지션)를 다시 전략에 전달하거나,
            # 전략이 PortfolioManager의 상태를 조회할 수 있도록 설계하는 것이 더 견고함.
            # 지금은 단순화를 위해 전략이 스스로 포지션 보유 여부를 트래킹하는 방식 (임시)
            # --> 백테스터가 매매 체결 후 전략에게 `on_trade_executed(stock_code, trade_type, quantity)`와 같은 메서드를 호출해주면 됨.
            # 또는 PortfolioManager의 get_holding_quantity를 호출하도록 변경.
            # 일단은 단순화된 로직으로 진행.
            
        return signals

    def on_minute_data(self, current_datetime: datetime, all_minute_data: Dict[str, pd.DataFrame]) -> List[dict]:
        """
        분별 데이터가 주어졌을 때 모든 종목에 대해 매매 신호를 생성합니다.
        """
        # 일봉 데이터와 동일한 로직을 사용하지만, current_datetime을 사용하고 minute_data를 전달합니다.
        # 이 예시 전략은 주로 일봉에 적합하지만, 분봉에도 적용 가능하도록 구조화합니다.
        signals = []
        for stock_code, data_df in all_minute_data.items():
            signal = self.generate_signal(stock_code, data_df)
            if signal and signal['signal'] != 'HOLD':
                signals.append(signal)
        return signals

    def on_finish(self):
        """
        백테스팅 종료 시 호출됩니다.
        """
        self.logger.info("MovingAverageCrossoverStrategy finished.")
        # 최종 상태 정리 등이 필요하면 여기에 추가