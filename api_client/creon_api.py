# backtest/api_client/creon_api.py

import win32com.client
import ctypes
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
import re

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CreonAPIClient:
    def __init__(self):
        self.connected = False
        # self.stock_chart = None # _get_price_data에서 새로 생성하므로 필요 없음
        self.cp_code_mgr = None
        self.cp_cybos = None
        self.stock_name_dic = {}
        self.stock_code_dic = {}
        self._connect_creon()
        if self.connected:
            self.cp_code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr") # 이 위치로 이동
            logger.info("CpCodeMgr COM object initialized.")
            self._make_stock_dic()

    def _connect_creon(self):
        """Creon Plus에 연결하고 COM 객체를 초기화합니다."""
        if ctypes.windll.shell32.IsUserAnAdmin():
            logger.info("Running with administrator privileges.")
        else:
            logger.warning("Not running with administrator privileges. Some Creon functions might be restricted.")

        self.cp_cybos = win32com.client.Dispatch("CpUtil.CpCybos")
        if self.cp_cybos.IsConnect:
            self.connected = True
            logger.info("Creon Plus is already connected.")
        else:
            logger.info("Attempting to connect to Creon Plus...")
            # self.cp_cybos.PlusConnect()
            max_retries = 10
            for i in range(max_retries):
                if self.cp_cybos.IsConnect:
                    self.connected = True
                    logger.info("Creon Plus connected successfully.")
                    break
                else:
                    logger.warning(f"Waiting for Creon Plus connection... ({i+1}/{max_retries})")
                    time.sleep(2)
            if not self.connected:
                logger.error("Failed to connect to Creon Plus. Please ensure HTS is running and logged in.")
                raise ConnectionError("Creon Plus connection failed.")

        # CpSysDib.StockChart는 _get_price_data에서 요청마다 새로 생성하므로 여기서는 초기화하지 않음.
        # self.stock_chart = win32com.client.Dispatch("CpSysDib.StockChart") # 더 이상 여기서 초기화하지 않음


    def _check_creon_status(self):
        """Creon API 사용 가능한지 상태를 확인합니다."""
        if not self.connected:
            logger.error("Creon Plus is not connected.")
            return False

        # 요청 제한 개수 확인 (대부분의 Creon API 버전에서 인자 없음)
        # remain_count = self.cp_cybos.GetLimitRequestRemainTime() # 인자 제거
        # if remain_count <= 0:
        #     logger.warning(f"Creon API request limit reached. Waiting for 1 second.")
        #     time.sleep(1)
        #     remain_count = self.cp_cybos.GetLimitRequestRemainTime() # 인자 제거
        #     if remain_count <= 0:
        #         logger.error("Creon API request limit still active after waiting. Cannot proceed.")
        #         return False
        return True

    def _is_spac(self, code_name):
        """종목명에 숫자+'호' 패턴이 있으면 스펙주로 판단합니다."""
        return re.search(r'\d+호', code_name) is not None

    def _is_preferred_stock(self, code_name):
        """더 포괄적인 우선주 판단"""
        return re.search(r'([0-9]+우|[가-힣]우[A-Z]?)$', code_name) is not None and len(code_name) >= 3

    def _is_reits(self, code_name):
        """종목명에 '리츠'가 포함되면 리츠로 판단합니다."""
        return "리츠" in code_name

    def _make_stock_dic(self):
        """주식 종목 정보를 딕셔너리로 저장합니다. 스펙주, 우선주, 리츠 제외."""
        logger.info("종목 코드/명 딕셔너리 생성 시작")
        if not self.cp_code_mgr:
            logger.error("cp_code_mgr is not initialized. Cannot make stock dictionary.")
            return

        try:
            kospi_codes = self.cp_code_mgr.GetStockListByMarket(1)
            kosdaq_codes = self.cp_code_mgr.GetStockListByMarket(2)
            all_codes = kospi_codes + kosdaq_codes
            
            processed_count = 0
            for code in all_codes:
                code_name = self.cp_code_mgr.CodeToName(code)
                section_kind = str(self.cp_code_mgr.GetStockSectionKind(code))

                if (section_kind != '1' or
                    self._is_spac(code_name) or
                    self._is_preferred_stock(code_name) or
                    self._is_reits(code_name)):
                    continue

                self.stock_name_dic[code_name] = code
                self.stock_code_dic[code] = code_name
                processed_count += 1

            logger.info(f"종목 코드/명 딕셔너리 생성 완료. 총 {processed_count}개 종목 저장.")

        except Exception as e:
            logger.error(f"_make_stock_dic 중 오류 발생: {e}", exc_info=True)

    def get_stock_name(self, find_code):
        """종목코드로 종목명을 반환 합니다."""
        return self.stock_code_dic.get(find_code, None)

    def get_stock_code(self, find_name):
        """종목명으로 종목목코드를 반환 합니다."""
        return self.stock_name_dic.get(find_name, None)

    def get_filtered_stock_list(self):
        """필터링된 모든 종목 코드를 리스트로 반환합니다."""
        return list(self.stock_code_dic.keys())


    def _get_price_data(self, stock_code, period, from_date_str, to_date_str, interval=1):
        """
        Creon API에서 주식 차트 데이터를 가져오는 내부 범용 메서드.
        :param stock_code: 종목 코드 (예: 'A005930')
        :param period: 'D': 일봉, 'W': 주봉, 'M': 월봉, 'm': 분봉
        :param from_date_str: 시작일 (YYYYMMDD 형식 문자열)
        :param to_date_str: 종료일 (YYYYMMDD 형식 문자열)
        :param interval: 분봉일 경우 주기 (기본 1분)
        :return: Pandas DataFrame
        """
        if not self._check_creon_status():
            return pd.DataFrame()

        objChart = win32com.client.Dispatch('CpSysDib.StockChart')
        
        # 입력 값 설정
        objChart.SetInputValue(0, stock_code)
        objChart.SetInputValue(1, ord('1'))    # 요청구분 1:기간 2: 개수 (우리는 기간으로 요청)
        objChart.SetInputValue(2, int(to_date_str))   # 2: To 날짜 (long)
        objChart.SetInputValue(3, int(from_date_str)) # 3: From 날짜 (long)
        objChart.SetInputValue(6, ord(period)) # 주기
        objChart.SetInputValue(9, ord('1'))    # 수정주가 사용

        # 요청 항목 설정 (주기에 따라 달라짐)
        if period == 'm':
            objChart.SetInputValue(7, interval)  # 분틱차트 주기 (1분)
            # 요청 항목: 날짜(0), 시간(1), 시가(2), 고가(3), 저가(4), 종가(5), 거래량(8)
            requested_fields = [0, 1, 2, 3, 4, 5, 8] # Creon API의 필드 인덱스
        else: # 일봉, 주봉, 월봉
            # 요청 항목: 날짜(0), 시가(2), 고가(3), 저가(4), 종가(5), 거래량(8)
            requested_fields = [0, 2, 3, 4, 5, 8]
            
        objChart.SetInputValue(5, requested_fields) # 요청할 데이터

        data_list = []
        
        while True:
            objChart.BlockRequest()
            time.sleep(0.2) # 과도한 요청 방지 및 제한 시간 준수

            rq_status = objChart.GetDibStatus()
            rq_msg = objChart.GetDibMsg1()

            if rq_status != 0:
                logger.error(f"CpStockChart: 데이터 요청 실패. 통신상태: {rq_status}, 메시지: {rq_msg}")
                # 오류 코드 5는 '해당 기간의 데이터 없음'을 의미할 수 있음
                if rq_status == 5:
                    logger.warning(f"No data for {stock_code} in specified period ({from_date_str}~{to_date_str}).")
                return pd.DataFrame() # 빈 DataFrame 반환

            received_len = objChart.GetHeaderValue(3) # 현재 BlockRequest로 수신된 데이터 개수
            if received_len == 0:
                break # 더 이상 받을 데이터가 없으면 루프 종료

            for i in range(received_len):
                if period == 'm':
                    date_val = objChart.GetDataValue(0, i) # 날짜 (YYYYMMDD)
                    time_val = objChart.GetDataValue(1, i) # 시간 (HHMM)
                    dt_str = f"{date_val}{time_val:04d}" # 시간을 4자리로 채움 (예: 930 -> 0930)
                    dt_obj = datetime.strptime(dt_str, '%Y%m%d%H%M')

                    data_list.append({
                        'stock_code': stock_code,
                        'datetime': dt_obj, # 분봉은 datetime 컬럼
                        'open_price': objChart.GetDataValue(2, i),
                        'high_price': objChart.GetDataValue(3, i),
                        'low_price': objChart.GetDataValue(4, i),
                        'close_price': objChart.GetDataValue(5, i),
                        'volume': objChart.GetDataValue(6, i) # 필드 8(거래량)의 실제 인덱스는 6
                        # 'trading_value': None # 분봉에서는 trading_value를 요청하지 않음 (요청 필드에 8이 없으므로)
                    })
                else: # 일봉, 주봉, 월봉
                    date = objChart.GetDataValue(0, i)
                    data_list.append({
                        'stock_code': stock_code,
                        'date': datetime.strptime(str(date), '%Y%m%d').date(), # 일봉은 date 컬럼
                        'open_price': objChart.GetDataValue(1, i), # 필드 1의 실제 인덱스는 1
                        'high_price': objChart.GetDataValue(2, i), # 필드 2의 실제 인덱스는 2
                        'low_price': objChart.GetDataValue(3, i), # 필드 3의 실제 인덱스는 3
                        'close_price': objChart.GetDataValue(4, i), # 필드 4의 실제 인덱스는 4
                        'volume': objChart.GetDataValue(5, i), # 필드 5의 실제 인덱스는 5
                        'change_rate': None, # 추후 계산
                        'trading_value': None # 필드 8(거래대금)의 실제 인덱스는 6
                    })
            
            if not objChart.Continue:
                break # 더 이상 연속 조회할 데이터가 없으면 종료

        df = pd.DataFrame(data_list)
        # Creon API는 최신 데이터부터 과거 데이터 순으로 반환하므로, 오름차순으로 정렬
        if not df.empty:
            if period == 'm':
                df = df.sort_values(by='datetime').reset_index(drop=True)
            else:
                df = df.sort_values(by='date').reset_index(drop=True)
        return df

    def get_daily_ohlcv(self, stock_code, start_date_str, end_date_str):
        """
        특정 종목의 일봉 OHLCV 데이터를 Creon API에서 가져옵니다.
        :param stock_code: 종목 코드 (예: 'A005930')
        :param start_date_str: 시작일 (YYYYMMDD 형식 문자열)
        :param end_date_str: 종료일 (YYYYMMDD 형식 문자열)
        :return: Pandas DataFrame
        """
        logger.info(f"Fetching daily data for {stock_code} from {start_date_str} to {end_date_str}")
        return self._get_price_data(stock_code, 'D', start_date_str, end_date_str)

    def get_minute_ohlcv(self, stock_code, start_date_str, end_date_str, interval=1):
        """
        특정 종목의 분봉 OHLCV 데이터를 Creon API에서 가져옵니다.
        :param stock_code: 종목 코드 (예: 'A005930')
        :param start_date_str: 시작일 (YYYYMMDD 형식 문자열)
        :param end_date_str: 종료일 (YYYYMMDD 형식 문자열)
        :param interval: 분봉 주기 (기본 1분)
        :return: Pandas DataFrame
        """
        logger.info(f"Fetching {interval}-minute data for {stock_code} from {start_date_str} to {end_date_str}")
        return self._get_price_data(stock_code, 'm', start_date_str, end_date_str, interval)