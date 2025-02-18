# config.py

# 아래 설정들은 다양한 시간 프레임(5m, 15m, 1h, 4h 등)에서
# 실전매매와 유사한 비용 구조를 적용하기 위한 값들입니다.
# ALLOW_SHORT, LEVERAGE 등을 통해 숏/레버리지 여부를 쉽게 토글할 수 있습니다.

SYMBOL = "BTCUSDT"    # 매매 대상 심볼 (선물/현물 공통, 여기서는 바이낸스 선물 BTCUSDT 예시)
FEE_RATE = 0.0004     # 체결 시 발생하는 수수료(0.0004 = 0.04%). 매수/청산 각각 적용됨
BID_ASK_SPREAD = 0.0005  # 호가 스프레드(0.0005 = 0.05%). 진입/청산 가격을 불리하게 조정할 때 사용
MARGIN_INTEREST = 0.0001  # 포지션 보유 중 발생하는 마진/차입 이자 (캔들 단위로 누적 차감 가능)
SLIPPAGE_RATE = 0.0002    # 체결 시 발생하는 슬리피지(0.0002 = 0.02%). 스프레드와 함께 체결가격 보정 시 사용

LEVERAGE = 1         # 레버리지 배수 (1이면 미사용, 2 이상으로 설정 시 2배 레버리지 등)
ALLOW_SHORT = True   # True면 숏 포지션 허용. False면 인-아웃(현금/롱)만 가능
INIT_CAPITAL = 100000 # 초기 자본금 (USD 기준 예시)

START_DATE = "2024-01-01" # 백테스트 시작일
END_DATE   = "2024-12-31" # 백테스트 종료일

TRAIN_RATIO = 0.7    # 전체 데이터 중 앞 70% 구간을 학습(In-Sample), 뒤 30%를 검증(OOS)으로 사용

DATA_PATH    = "./data/"    # 데이터 저장 폴더
RESULTS_PATH = "./results/" # 백테스트 결과 저장 폴더

# 가능한 모든 타임프레임 리스트 (5m ~ 1M 등)
POSSIBLE_TIMEFRAMES = ["5m","15m","1h","4h","1d"]
# 실제 사용할 타임프레임(예: 4h만 선택 중)
TIMEFRAMES = ["4h"]

# 각 지표별 파라미터 설정: 이동평균(MA), RSI, Support/Resistance 등
# 밑의 파라미터 조합을 모두 순회하여 다양한 전략을 테스트할 수 있습니다.
# 논문("75,360 조합.pdf")에서 설정한 band_filter, delay_filter, holding_period
# 값만 반영한 예시입니다. 나머지 파라미터(short_periods, long_periods 등)는
# 기존과 동일하게 유지합니다.

INDICATOR_PARAMS = {
    "MA": {
        # 기존 (5,8,9,10,13,20,50)에 21,34 추가
        "short_periods": [5,8,9,10,13,20,21,34,50],
        "long_periods":  [50,100,150,200],  # 그대로
        "band_filter":   [0, 0.05, 0.1],
        "delay_filter":  [0,1,2],
        "holding_period":[6,12,'inf']
    },
    "RSI": {
        # 기존 (7,8,9,12,14,21)에 25,34 추가
        "lengths":       [7,8,9,12,14,21,25,34],
        "overbought_values": [65,70,75,80,85],
        "oversold_values":   [15,20,25,30,35],
        "band_filter":   [0, 0.05, 0.1],
        "delay_filter":  [0,1,2],
        "holding_period":[6,12,'inf']
    },
    "Support_Resistance": {
        # 기존 (5,10,13,20,30,50)에 21,34 추가
        "windows":       [5,10,13,20,21,30,34,50],
        "band_filter":   [0, 0.05, 0.1],
        "delay_filter":  [0,1,2],
        "holding_period":[6,12,'inf']
    },
    "Filter_Rule": {
        # 기존 (5,10,13,20,30)에 21,34 추가
        "windows":       [5,10,13,20,21,30,34],
        "x_values":      [0.05,0.1,0.15],
        "y_values":      [0.05,0.1,0.15],
        "band_filter":   [0, 0.05, 0.1],
        "delay_filter":  [0,1,2],
        "holding_period":[6,12,'inf']
    },
    "Channel_Breakout": {
        # 기존 (10,13,20,30,55)에 21,34 추가
        "windows":       [10,13,20,21,30,34,55],
        "c_values":      [0.05,0.1,0.15],
        "band_filter":   [0, 0.05, 0.1],
        "delay_filter":  [0,1,2],
        "holding_period":[6,12,'inf']
    },
    "OBV": {
        # 기존 (5,9,10,15,20)에 21,34 추가
        "short_periods": [5,9,10,15,20,21,34],
        "long_periods":  [50,100,150,200],
        "band_filter":   [0, 0.05, 0.1],
        "delay_filter":  [0,1,2],
        "holding_period":[6,12,'inf']
    }
}


