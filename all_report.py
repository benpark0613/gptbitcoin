if __name__ == "__main__":
    print("Starting Binance Futures Report...")
    futures_report_binance.main()  # 바이낸스 선물 리포트 생성

    print("\nStarting Upbit Spot Report...")
    spot_report_upbit.main()  # 업비트 현물 리포트 생성

    print("\nAll reports have been successfully generated!")
