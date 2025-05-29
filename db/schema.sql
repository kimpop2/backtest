-- backtest/db/schema.sql

-- 데이터베이스 생성 (필요시 주석 해제 후 사용)
CREATE DATABASE IF NOT EXISTS backtest_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE backtest_db;

SET FOREIGN_KEY_CHECKS = 0; -- 외래 키 검사 일시 비활성화

DROP TABLE IF EXISTS trade_log;
DROP TABLE IF EXISTS backtest_results;
DROP TABLE IF EXISTS minute_stock_data;
DROP TABLE IF EXISTS daily_stock_data;
DROP TABLE IF EXISTS stock_info;

SET FOREIGN_KEY_CHECKS = 1; -- 외래 키 검사 다시 활성화

-- 1. 종목 정보 테이블
-- Creon API를 통해 얻는 종목 코드, 종목명, 시장 구분 등을 저장
CREATE TABLE IF NOT EXISTS stock_info (
    stock_code VARCHAR(10) PRIMARY KEY NOT NULL, -- 종목 코드 (예: A005930)
    stock_name VARCHAR(100) NOT NULL,            -- 종목명 (예: 삼성전자)
    market_type VARCHAR(10) NOT NULL,             -- 시장 구분 (예: 1:KOSPI, 2:KOSDAQ)
    sector VARCHAR(100),                         -- 섹터 (선택적)
    per DECIMAL(10, 2),                          -- PER (선택적, 기본적 분석용)
    pbr DECIMAL(10, 2),                          -- PBR (선택적, 기본적 분석용)
    eps DECIMAL(15, 2),                          -- EPS (선택적, 기본적 분석용)
    -- 필요한 추가 정보 (상장일, 액면가 등)
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 2. 일봉(Daily) 주식 데이터 테이블
-- 각 종목의 일별 OHLCV (Open, High, Low, Close, Volume) 데이터
CREATE TABLE IF NOT EXISTS daily_stock_data (
    stock_code VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price INT NOT NULL,
    high_price INT NOT NULL,
    low_price INT NOT NULL,
    close_price INT NOT NULL,
    volume BIGINT NOT NULL,
    change_rate DECIMAL(10, 2), -- 전일 대비 등락률
    trading_value BIGINT,       -- 거래 대금
    PRIMARY KEY (stock_code, date) -- 종목 코드와 날짜 조합으로 유니크
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. 분봉(Minute) 주식 데이터 테이블 (선택적: 필요시 나중에 추가)
-- 각 종목의 분별 OHLCV (Open, High, Low, Close, Volume) 데이터
CREATE TABLE IF NOT EXISTS minute_stock_data (
    stock_code VARCHAR(10) NOT NULL,
    datetime DATETIME NOT NULL,
    open_price INT NOT NULL,
    high_price INT NOT NULL,
    low_price INT NOT NULL,
    close_price INT NOT NULL,
    volume BIGINT NOT NULL,
    PRIMARY KEY (stock_code, datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 4. 백테스팅 결과 테이블
-- 각 백테스팅 실행의 요약 결과 저장 (예: 전략별, 기간별 성능)
CREATE TABLE IF NOT EXISTS backtest_results (
    result_id INT AUTO_INCREMENT PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,        -- 사용된 전략명
    start_date DATE NOT NULL,                   -- 백테스팅 시작일
    end_date DATE NOT NULL,                     -- 백테스팅 종료일
    initial_capital BIGINT NOT NULL,            -- 초기 투자금
    final_capital BIGINT NOT NULL,              -- 최종 자산
    total_return DECIMAL(10, 2) NOT NULL,       -- 총 수익률 (%)
    annualized_return DECIMAL(10, 2),           -- 연평균 수익률 (CAGR) (%)
    max_drawdown DECIMAL(10, 2),                -- 최대 낙폭 (MDD) (%)
    sharpe_ratio DECIMAL(10, 4),                -- 샤프 비율
    total_trades INT,                           -- 총 거래 횟수
    win_rate DECIMAL(5, 2),                     -- 승률 (%)
    profit_factor DECIMAL(10, 2),               -- 손익비
    commission_rate DECIMAL(5, 4) NOT NULL,     -- 적용된 수수료율
    slippage_rate DECIMAL(5, 4),                -- 적용된 슬리피지율
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 5. 백테스팅 상세 거래 내역 테이블
-- 각 백테스팅 내에서 발생한 개별 거래 기록
CREATE TABLE IF NOT EXISTS trade_log (
    trade_id INT AUTO_INCREMENT PRIMARY KEY,
    result_id INT NOT NULL,                     -- backtest_results 테이블 참조
    stock_code VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,                   -- 거래 발생 일자
    trade_type VARCHAR(10) NOT NULL,            -- 'BUY' 또는 'SELL'
    price DECIMAL(15, 2) NOT NULL,              -- 거래 가격
    quantity INT NOT NULL,                      -- 거래 수량
    commission DECIMAL(15, 2) DEFAULT 0,        -- 발생한 수수료
    slippage DECIMAL(15, 2) DEFAULT 0,          -- 발생한 슬리피지
    pnl DECIMAL(15, 2),                         -- 해당 거래의 손익 (청산 시 기록)
    position_size INT,                          -- 거래 후 포지션 크기
    portfolio_value BIGINT,                     -- 거래 시점 전체 포트폴리오 가치
    FOREIGN KEY (result_id) REFERENCES backtest_results(result_id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
