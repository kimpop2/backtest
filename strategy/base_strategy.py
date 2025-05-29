# backtest/strategy/base_strategy.py

from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime, date # datetime.date 임포트 추가
from typing import Dict, List 

class BaseStrategy(ABC):
    """
    모든 백테스팅 전략의 기본 추상 클래스입니다.
    새로운 전략을 구현할 때는 이 클래스를 상속받아 필요한 메서드를 오버라이드해야 합니다.
    """
    def __init__(self, name: str, params: dict = None):
        self.name = name
        self.params = params if params is not None else {}
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """전략별 로거 설정 (선택 사항, 필요시 구현)"""
        import logging
        logger = logging.getLogger(f"Strategy.{self.name}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    @abstractmethod
    def generate_signal(self, stock_code: str, current_data: pd.DataFrame) -> dict:
        """
        주어진 시장 데이터를 기반으로 특정 종목에 대한 매매 신호를 생성합니다.

        :param stock_code: 현재 신호를 생성할 종목 코드.
        :param current_data: 해당 stock_code에 대한 현재 시점까지의 OHLCV 데이터 (pandas DataFrame).
                             인덱스는 datetime/date, 컬럼은 'open_price', 'high_price', 'low_price', 'close_price', 'volume' 등을 포함합니다.
        :return: 매매 신호를 담은 딕셔너리. 예:
                 {'signal': 'BUY', 'stock_code': 'A005930', 'price': 50000, 'quantity': 10}
                 {'signal': 'SELL', 'stock_code': 'A005930', 'price': 51000, 'quantity': 10}
                 {'signal': 'HOLD'} 또는 {} (신호 없음, 또는 매매 없을 시)
        """
        pass

    @abstractmethod
    def on_init(self, initial_capital: float, stock_list: List[str]):
        """
        백테스팅 시작 시 전략을 초기화합니다.
        필요한 초기 설정 (예: 초기 포트폴리오 상태, 지표 계산을 위한 초기 데이터)을 수행합니다.

        :param initial_capital: 백테스팅 시작 시 초기 자본금
        :param stock_list: 백테스트 대상이 되는 모든 종목 코드 리스트 (예: ['A005930', 'A000660'])
        """
        pass

    @abstractmethod
    def on_daily_data(self, current_date: date, all_daily_data: Dict[str, pd.DataFrame]):
        """
        일별 데이터가 주어졌을 때 실행되는 로직입니다.
        주로 일봉 데이터 기반 전략에 사용되며, 해당 날짜에 대한 모든 종목의 데이터를 처리합니다.

        :param current_date: 현재 처리 중인 날짜
        :param all_daily_data: 백테스트 대상 모든 종목에 대한 현재 날짜까지의 누적 일봉 데이터 딕셔너리.
                               Key: stock_code, Value: 해당 종목의 pandas DataFrame (인덱스: date).
        :return: 매매 신호 리스트 또는 빈 리스트. 여러 종목에서 동시에 신호가 발생할 수 있음.
                 예: [{'signal': 'BUY', 'stock_code': 'A005930', ...}, {'signal': 'SELL', 'stock_code': 'A000660', ...}]
        """
        pass

    @abstractmethod
    def on_minute_data(self, current_datetime: datetime, all_minute_data: Dict[str, pd.DataFrame]):
        """
        분별 데이터가 주어졌을 때 실행되는 로직입니다.
        주로 분봉 데이터 기반 전략에 사용되며, 해당 시간에 대한 모든 종목의 데이터를 처리합니다.

        :param current_datetime: 현재 처리 중인 날짜/시간
        :param all_minute_data: 백테스트 대상 모든 종목에 대한 현재 날짜/시간까지의 누적 분봉 데이터 딕셔너리.
                                Key: stock_code, Value: 해당 종목의 pandas DataFrame (인덱스: datetime).
        :return: 매매 신호 리스트 또는 빈 리스트. 여러 종목에서 동시에 신호가 발생할 수 있음.
                 예: [{'signal': 'BUY', 'stock_code': 'A005930', ...}, {'signal': 'SELL', 'stock_code': 'A000660', ...}]
        """
        pass

    @abstractmethod
    def on_finish(self):
        """
        백테스팅 종료 시 호출되는 로직입니다.
        최종 상태 정리, 결과 계산 등에 사용됩니다.
        """
        pass

    def set_params(self, params: dict):
        """전략의 파라미터를 설정합니다."""
        self.params.update(params)
        self.logger.info(f"{self.name} strategy parameters updated: {self.params}")

    def get_name(self) -> str:
        """전략의 이름을 반환합니다."""
        return self.name

    def get_params(self) -> dict:
        """전략의 현재 파라미터를 반환합니다."""
        return self.params