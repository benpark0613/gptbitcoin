import os
import requests
import pandas as pd
import calendar
from datetime import datetime


class BinanceOptionsDataFetcher:
    def __init__(self):
        self.base_url = "https://eapi.binance.com/eapi/v1"
        self.save_path = "btc_options_data"
        os.makedirs(self.save_path, exist_ok=True)

    def save_to_csv(self, df, filename):
        """데이터를 CSV 파일로 저장"""
        if df is not None and not df.empty:
            filepath = os.path.join(self.save_path, filename)
            df.to_csv(filepath, index=False, encoding="utf-8-sig")
            print(f"CSV 저장 완료: {filepath}")

    def get_options_data(self):
        """
        옵션 티커 데이터를 가져와서 DataFrame으로 변환합니다.
        필요한 컬럼만 선택하고, BTC 옵션(심볼이 'BTC-'로 시작하는 경우)만 필터링합니다.
        이 데이터는 거래량, 풋/콜 비율, 행사가격 분포 계산에 활용됩니다.
        """
        try:
            ticker_endpoint = f"{self.base_url}/ticker"
            response = requests.get(ticker_endpoint)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            # 필요한 컬럼만 선택
            columns = ['symbol', 'lastPrice', 'volume', 'strikePrice', 'bidPrice', 'askPrice']
            df = df[columns]
            # 숫자형 컬럼 변환
            numeric_columns = ['lastPrice', 'volume', 'strikePrice', 'bidPrice', 'askPrice']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col])
            # BTC 옵션만 필터링 (심볼이 "BTC-"로 시작)
            df_btc = df[df['symbol'].str.startswith("BTC-")].reset_index(drop=True)
            print(f"비트코인 옵션 데이터 조회 완료: {len(df_btc)} 건")
            return df_btc
        except Exception as e:
            print(f"에러 발생 (옵션 데이터 조회 실패): {e}")
            return None

    def get_open_interest(self, expiration="250214"):
        """
        BTC 옵션 미결제약정(Open Interest, OI) 데이터 조회
        (expiration은 YYMMDD 형식; 필요에 따라 수정)
        """
        try:
            oi_endpoint = f"{self.base_url}/openInterest?underlyingAsset=BTC&expiration={expiration}"
            response = requests.get(oi_endpoint)
            response.raise_for_status()
            oi_data = response.json()
            df_oi = pd.DataFrame(oi_data)
            print(f"BTC 옵션 미결제약정(OI) 데이터 조회 완료: {len(df_oi)} 건")
            self.save_to_csv(df_oi, "btc_open_interest.csv")
            return df_oi
        except Exception as e:
            print(f"에러 발생 (OI 요청 실패): {e}")
            return None

    def get_implied_volatility(self, symbol):
        """
        BTC 옵션 암시적 변동성(IV) 데이터 조회
        symbol은 옵션 티커 (예: "BTC-220624-9000-C") 형태로 사용합니다.
        """
        try:
            iv_endpoint = f"{self.base_url}/mark?symbol={symbol}"
            response = requests.get(iv_endpoint)
            response.raise_for_status()
            iv_data = response.json()
            df_iv = pd.DataFrame(iv_data)
            print(f"BTC 옵션 ({symbol}) 암시적 변동성(IV) 데이터 조회 완료")
            self.save_to_csv(df_iv, "btc_implied_volatility.csv")
            return df_iv
        except Exception as e:
            print(f"에러 발생 (IV 요청 실패): {e}")
            return None

    def get_put_call_ratio(self, df):
        """
        풋/콜 비율(Put/Call Ratio)을 계산합니다.
        심볼의 끝이 "-P"인 경우 풋, "-C"인 경우 콜로 분류하여 거래량을 합산합니다.
        """
        try:
            df_put = df[df['symbol'].str.endswith("-P")]
            df_call = df[df['symbol'].str.endswith("-C")]
            put_volume = df_put['volume'].sum()
            call_volume = df_call['volume'].sum()
            put_call_ratio = put_volume / call_volume if call_volume > 0 else None
            df_ratio = pd.DataFrame([{"Put/Call Ratio": put_call_ratio}])
            print(f"풋/콜 비율: {put_call_ratio:.2f}")
            self.save_to_csv(df_ratio, "btc_put_call_ratio.csv")
            return put_call_ratio
        except Exception as e:
            print(f"에러 발생 (PCR 계산 실패): {e}")
            return None

    def get_options_distribution(self, df):
        """
        옵션 행사가격 분포 데이터를 계산합니다.
        strikePrice별 옵션 개수를 집계하여 CSV로 저장합니다.
        """
        try:
            df_distribution = df.groupby('strikePrice').size().reset_index(name='count')
            print("옵션 행사가격 분포 데이터 조회 완료")
            self.save_to_csv(df_distribution, "btc_strike_distribution.csv")
            return df_distribution
        except Exception as e:
            print(f"에러 발생 (행사가격 분포 계산 실패): {e}")
            return None

    def get_option_volume(self, df):
        """
        옵션 거래량 데이터를 조회합니다.
        각 옵션 심볼별 거래량을 CSV로 저장합니다.
        """
        try:
            df_volume = df[['symbol', 'volume']].copy()
            print("BTC 옵션 거래량 데이터 조회 완료")
            self.save_to_csv(df_volume, "btc_option_volume.csv")
            return df_volume
        except Exception as e:
            print(f"에러 발생 (옵션 거래량 조회 실패): {e}")
            return None

    def save_report_option_txt(self, df_btc, df_oi, df_iv, put_call_ratio, df_distribution, df_volume):
        """
        옵션 데이터를 report_option.txt 파일로 저장합니다.
        각 섹션별로 CSV 형식의 데이터를 기록합니다.
        """
        report_file = os.path.join(self.save_path, "report_option.txt")
        with open(report_file, 'w', encoding="utf-8", newline="") as f:
            # BTC 옵션 데이터
            f.write("-- BTC 옵션 데이터 (CSV)\n")
            if df_btc is not None and not df_btc.empty:
                f.write(df_btc.to_csv(sep=";", index=False))
            else:
                f.write("No BTC 옵션 데이터")
            f.write("\n\n")

            # 미결제약정 데이터
            f.write("-- BTC 옵션 미결제약정 (Open Interest) (CSV)\n")
            if df_oi is not None and not df_oi.empty:
                f.write(df_oi.to_csv(sep=";", index=False))
            else:
                f.write("No Open Interest Data")
            f.write("\n\n")

            # 암시적 변동성 데이터
            f.write("-- BTC 옵션 암시적 변동성 (IV) (CSV)\n")
            if df_iv is not None and not df_iv.empty:
                f.write(df_iv.to_csv(sep=";", index=False))
            else:
                f.write("No IV Data")
            f.write("\n\n")

            # Put/Call Ratio
            f.write("-- BTC 옵션 Put/Call Ratio\n")
            if put_call_ratio is not None:
                f.write(str(put_call_ratio))
            else:
                f.write("No Put/Call Ratio Data")
            f.write("\n\n")

            # 옵션 행사가격 분포 데이터
            f.write("-- BTC 옵션 행사가격 분포 (CSV)\n")
            if df_distribution is not None and not df_distribution.empty:
                f.write(df_distribution.to_csv(sep=";", index=False))
            else:
                f.write("No Strike Distribution Data")
            f.write("\n\n")

            # 옵션 거래량 데이터
            f.write("-- BTC 옵션 거래량 (CSV)\n")
            if df_volume is not None and not df_volume.empty:
                f.write(df_volume.to_csv(sep=";", index=False))
            else:
                f.write("No Option Volume Data")
            f.write("\n\n")

        print(f"옵션 데이터 리포트 생성 완료: {report_file}")


if __name__ == "__main__":
    fetcher = BinanceOptionsDataFetcher()

    # 내부 계산용 옵션 티커 데이터 조회
    df_btc = fetcher.get_options_data()
    if df_btc is not None and not df_btc.empty:
        print("\n비트코인 옵션 데이터 샘플:")
        print(df_btc.head())

        # 1. 미결제약정 (Open Interest)
        df_oi = fetcher.get_open_interest(expiration="250214")

        # 2. 암시적 변동성 (IV)
        # 여기서는 옵션 데이터 중 첫 번째 심볼을 사용합니다.
        first_symbol = df_btc.iloc[0]['symbol']
        df_iv = fetcher.get_implied_volatility(first_symbol)

        # 3. 풋/콜 비율 (PCR)
        put_call_ratio = fetcher.get_put_call_ratio(df_btc)

        # 4. 옵션 행사가격 분포
        df_distribution = fetcher.get_options_distribution(df_btc)

        # 5. 옵션 거래량
        df_volume = fetcher.get_option_volume(df_btc)

        # report_option.txt 파일로 옵션 데이터 추가 저장
        fetcher.save_report_option_txt(df_btc, df_oi, df_iv, put_call_ratio, df_distribution, df_volume)
    else:
        print("비트코인 옵션 데이터를 가져오지 못했습니다.")
