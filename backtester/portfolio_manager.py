# backtest/backtester/portfolio_manager.py

import logging
from datetime import datetime
import pandas as pd
from typing import Dict, List 

logger = logging.getLogger(__name__)

class PortfolioManager:
    """
    백테스팅 중 포트폴리오의 자산, 현금, 보유 종목을 관리합니다.
    매매 신호에 따라 현금 흐름과 종목 보유 수량을 업데이트하고,
    수수료와 슬리피지를 적용합니다.
    """
    def __init__(self, initial_capital: float, commission_rate: float = 0.00015, slippage_rate: float = 0.0001):
        self.initial_capital = initial_capital
        self.current_cash = initial_capital
        self.holdings = {}  # {'stock_code': {'quantity': int, 'avg_price': float, 'current_price': float}}
        self.trade_logs = [] # 백테스팅 중 발생하는 모든 거래 기록
        self.portfolio_value_history = [] # 일별 또는 분별 포트폴리오 가치 변화
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.current_date = None # 현재 처리 중인 날짜 (일별/분별 백테스트 시 업데이트)
        
        logger.info(f"PortfolioManager initialized with initial capital: {initial_capital}")
        logger.info(f"Commission rate: {commission_rate}, Slippage rate: {slippage_rate}")

    def update_current_market_data(self, current_date_or_datetime: datetime, market_prices: Dict[str, float]):
        """
        현재 시장 가격을 업데이트하고 포트폴리오 가치를 계산합니다.
        :param current_date_or_datetime: 현재 처리 중인 날짜 또는 날짜/시간
        :param market_prices: {'stock_code': current_close_price} 딕셔너리
        """
        self.current_date = current_date_or_datetime
        
        total_holdings_value = 0
        for stock_code, holding_info in self.holdings.items():
            if stock_code in market_prices:
                current_price = market_prices[stock_code]
                holding_info['current_price'] = current_price
                total_holdings_value += holding_info['quantity'] * current_price
            # else: 보유하고 있는 종목의 가격 데이터가 없으면 해당 종목은 0으로 처리되거나 이전 가격 유지 (구현 선택)
            # 여기서는 편의상 없는 경우 건너뜀. 실제 환경에서는 에러 처리 필요.

        current_portfolio_value = self.current_cash + total_holdings_value
        self.portfolio_value_history.append({
            'date': current_date_or_datetime,
            'portfolio_value': current_portfolio_value,
            'cash': self.current_cash,
            'holdings_value': total_holdings_value
        })
        # logger.debug(f"[{current_date_or_datetime}] Portfolio Value: {current_portfolio_value:.2f}, Cash: {self.current_cash:.2f}")

    def execute_order(self, signal: dict):
        """
        매매 신호를 실행합니다.
        :param signal: {'signal': 'BUY'/'SELL', 'stock_code': 'A005930', 'price': 50000, 'quantity': 10}
        :return: bool, 거래 성공 여부
        """
        trade_type = signal.get('signal')
        stock_code = signal.get('stock_code')
        price = signal.get('price')
        quantity = signal.get('quantity')

        if not all([trade_type, stock_code, price, quantity]):
            logger.error(f"Invalid signal received: {signal}")
            return False

        commission = price * quantity * self.commission_rate
        slippage = price * quantity * self.slippage_rate # 슬리피지를 가격에 직접 반영하지 않고 총액에 계산
        
        total_cost = 0
        pnl = 0 # Profit and Loss for this specific trade

        if trade_type == 'BUY':
            total_cost = (price * quantity) + commission + slippage
            if self.current_cash >= total_cost:
                self.current_cash -= total_cost
                if stock_code not in self.holdings:
                    self.holdings[stock_code] = {'quantity': 0, 'avg_price': 0.0, 'current_price': price}
                
                # 평단가 계산
                existing_total_value = self.holdings[stock_code]['quantity'] * self.holdings[stock_code]['avg_price']
                new_total_value = existing_total_value + (price * quantity)
                new_total_quantity = self.holdings[stock_code]['quantity'] + quantity
                self.holdings[stock_code]['avg_price'] = new_total_value / new_total_quantity if new_total_quantity > 0 else 0.0
                self.holdings[stock_code]['quantity'] += quantity
                
                logger.info(f"[{self.current_date}] BUY {stock_code}: {quantity} @ {price:.2f} (Cash: {self.current_cash:.2f}, Holdings: {self.holdings[stock_code]['quantity']})")
                trade_success = True
            else:
                logger.warning(f"[{self.current_date}] Insufficient cash to BUY {stock_code}: {quantity} @ {price:.2f}. Required: {total_cost:.2f}, Available: {self.current_cash:.2f}")
                trade_success = False
        
        elif trade_type == 'SELL':
            if stock_code in self.holdings and self.holdings[stock_code]['quantity'] >= quantity:
                # 수익 계산 (매도 평단가 vs 매수 평단가)
                cost_of_sold_shares = self.holdings[stock_code]['avg_price'] * quantity
                revenue_from_sale = price * quantity
                pnl = revenue_from_sale - cost_of_sold_shares - commission - slippage # 손익 계산 시 수수료/슬리피지 포함

                self.current_cash += (price * quantity) - commission - slippage
                self.holdings[stock_code]['quantity'] -= quantity

                if self.holdings[stock_code]['quantity'] == 0:
                    del self.holdings[stock_code] # 전량 매도 시 홀딩스에서 제거
                else:
                    self.holdings[stock_code]['avg_price'] = self.holdings[stock_code]['avg_price'] # 매도 시 평단가 변화 없음
                
                logger.info(f"[{self.current_date}] SELL {stock_code}: {quantity} @ {price:.2f} (Cash: {self.current_cash:.2f}, Holdings: {self.get_holding_quantity(stock_code)}, PnL: {pnl:.2f})")
                trade_success = True
            else:
                logger.warning(f"[{self.current_date}] Insufficient {stock_code} to SELL. Holding: {self.get_holding_quantity(stock_code)}, Attempting to sell: {quantity}")
                trade_success = False
        else:
            logger.warning(f"[{self.current_date}] Unknown trade type: {trade_type} for {stock_code}. Skipping order.")
            trade_success = False
            
        if trade_success:
            self.trade_logs.append({
                'trade_type': trade_type,
                'stock_code': stock_code,
                'trade_date': self.current_date, # 백테스터에서 현재 날짜/시간 전달받아 사용
                'price': price,
                'quantity': quantity,
                'commission': commission,
                'slippage': slippage,
                'pnl': pnl, # 매도 시에만 의미있는 값
                'position_size': self.get_holding_quantity(stock_code),
                'portfolio_value': self.get_current_portfolio_value() # 거래 직후의 포트폴리오 가치
            })
        return trade_success

    def get_holding_quantity(self, stock_code: str) -> int:
        """현재 보유하고 있는 특정 종목의 수량을 반환합니다."""
        return self.holdings.get(stock_code, {}).get('quantity', 0)

    def get_current_portfolio_value(self) -> float:
        """현재 포트폴리오의 총 가치 (현금 + 종목 평가액)를 반환합니다."""
        total_holdings_value = sum(
            info['quantity'] * info['current_price']
            for info in self.holdings.values()
            if 'current_price' in info # current_price가 업데이트되었는지 확인
        )
        return self.current_cash + total_holdings_value

    def get_final_results(self) -> dict:
        """백테스팅 종료 시 최종 결과를 요약하여 반환합니다."""
        final_value = self.get_current_portfolio_value()
        total_return = ((final_value - self.initial_capital) / self.initial_capital) * 100 if self.initial_capital > 0 else 0
        
        # 일별/분별 포트폴리오 가치 변동 DataFrame
        if not self.portfolio_value_history:
            pv_df = pd.DataFrame(columns=['date', 'portfolio_value'])
        else:
            pv_df = pd.DataFrame(self.portfolio_value_history)
            pv_df['date'] = pd.to_datetime(pv_df['date'])
            pv_df.set_index('date', inplace=True)
        
        # MDD 계산 (PerformanceAnalyzer에서 더 상세하게 계산할 예정이지만, 여기에 기본값 포함)
        max_drawdown = 0.0
        if not pv_df.empty:
            peak_value = pv_df['portfolio_value'].expanding().max()
            drawdown = (pv_df['portfolio_value'] - peak_value) / peak_value
            max_drawdown = drawdown.min() * 100 if not drawdown.empty else 0.0

        # 간단한 승률 계산 (trade_log 기반)
        winning_trades = [t for t in self.trade_logs if t.get('pnl', 0) > 0 and t['trade_type'] == 'SELL']
        total_selling_trades = [t for t in self.trade_logs if t['trade_type'] == 'SELL']
        win_rate = (len(winning_trades) / len(total_selling_trades)) * 100 if total_selling_trades else 0

        return {
            'initial_capital': self.initial_capital,
            'final_capital': final_value,
            'total_return': total_return,
            'total_trades': len(self.trade_logs),
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'commission_rate': self.commission_rate,
            'slippage_rate': self.slippage_rate
            # 다른 지표 (CAGR, Sharpe Ratio, Profit Factor)는 PerformanceAnalyzer에서 계산
        }

    def get_trade_logs(self) -> List[dict]:
        """기록된 모든 거래 로그를 반환합니다."""
        return self.trade_logs

    def get_portfolio_value_history(self) -> pd.DataFrame:
        """포트폴리오 가치 변동 기록을 DataFrame으로 반환합니다."""
        if not self.portfolio_value_history:
            return pd.DataFrame(columns=['date', 'portfolio_value', 'cash', 'holdings_value'])
        pv_df = pd.DataFrame(self.portfolio_value_history)
        pv_df['date'] = pd.to_datetime(pv_df['date'])
        pv_df.set_index('date', inplace=True)
        return pv_df