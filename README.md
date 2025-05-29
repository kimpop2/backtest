# 주식 매매 전략 백테스팅 시뮬레이터

## 프로젝트 개요

이 프로젝트는 파이썬을 기반으로 한 주식 매매 전략 백테스팅 시뮬레이터입니다. 대신증권 Creon API를 통해 주식 데이터를 수집하고, `backtrader` 프레임워크를 사용하여 다양한 매매 전략을 백테스팅하며, 그 결과를 시각화하고 분석할 수 있는 기능을 제공합니다.

## 주요 기능 (MVP)

* **데이터 수집 및 관리**: 대신증권 Creon API를 통한 주식 일봉/분봉 데이터 수집 및 MariaDB 저장.
* **매매 대상 종목 유니버스 선정**: 특정 기준에 따라 백테스팅 대상 종목군을 선정.
* **백테스팅 엔진**: `backtrader`를 활용한 매매 전략 백테스팅 실행.
* **주기적인 포트폴리오 리밸런싱**: 동적 전략에 따라 포트폴리오 종목 및 비중 조정.
* **결과 분석 및 시각화**: 백테스팅 결과의 핵심 성과 지표(수익률, MDD 등) 계산 및 수익률 곡선 차트 제공.
* **간단한 GUI**: PyQt5를 이용한 기본적인 사용자 인터페이스.

## 개발 환경

* **언어**: Python 3.x
* **GUI**: PyQt5
* **백테스팅**: backtrader
* **데이터베이스**: MariaDB
* **API**: 대신증권 Creon API

## 설치 및 실행 방법

1.  **프로젝트 클론 (또는 다운로드):**
    ```bash
    git clone [https://github.com/kimpop2/backtest.git](https://github.com/kimpop2/backtest.git)
    cd backtest
    ```
2.  **Anaconda Python 가상 환경 설정 (`system_trading_py37_32`):**
    Anaconda Prompt 또는 터미널에서 다음 명령어를 사용하여 가상 환경을 생성하고 활성화합니다. 이미 환경이 존재한다면 활성화만 진행합니다.
    ```bash
    # 가상 환경 생성 (최초 1회)
    conda create -n system_trading_py37_32 python=3.7

    # 가상 환경 활성화
    conda activate system_trading_py37_32
    ```
    *VS Code에서 작업 시, 하단 상태바의 Python 인터프리터 선택기(`Python 3.x.x 64-bit` 등으로 표시된 부분)를 클릭하여 `system_trading_py37_32` 환경을 선택해 주세요.*

3.  **의존성 라이브러리 설치:**
    가상 환경이 활성화된 상태에서 `requirements.txt`에 명시된 라이브러리들을 설치합니다.
    ```bash
    pip install -r requirements.txt
    ```
    (참고: `PyQt5`는 `pip` 설치 시 간혹 문제가 있을 수 있습니다. `conda install pyqt` 또는 `pip install pyqt5`를 시도해 보세요.)

4.  **MariaDB 설정:**
    * MariaDB 서버를 설치하고 실행합니다.
    * `db/schema.sql` 파일을 사용하여 데이터베이스 스키마를 생성합니다.
    * `.env` 파일을 생성하고 DB 사용자명과 비밀번호를 설정합니다. (`.env` 파일은 Git에 포함되지 않습니다.)
        ```
        # .env
        DB_USER=your_db_username
        DB_PASSWORD=your_db_password
        ```
5.  **대신증권 Creon HTS 설치 및 API 설정:**
    * 대신증권 Creon HTS를 설치하고, API 사용을 위한 설정을 완료합니다.
    * (필요시 Creon API 가이드를 참고하여 COM 모듈 등록 등 추가 설정 진행)
6.  **애플리케이션 실행:**
    ```bash
    python main.py
    ```

## 프로젝트 기여

(선택 사항: 향후 기여자를 위한 가이드라인 추가)