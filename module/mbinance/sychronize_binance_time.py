import requests
import subprocess
import platform
from datetime import datetime


def sync_binance_time():
    url = "https://api.binance.com/api/v3/time"
    response = requests.get(url)
    data = response.json()

    # 바이낸스 서버 시간(밀리초)을 초 단위로 변환
    server_time_ms = data["serverTime"]
    server_time_s = server_time_ms / 1000.0

    # UTC 기준 datetime 객체 생성
    utc_datetime = datetime.utcfromtimestamp(server_time_s)

    # OS 체크
    current_os = platform.system().lower()

    if "windows" in current_os:
        # 윈도우용 날짜/시간 포맷 (UTC 시간을 그대로 쓰는 예시)
        # 로컬타임으로 맞추려면 utc_datetime.astimezone() 사용
        date_str = utc_datetime.strftime("%m-%d-%Y")
        time_str = utc_datetime.strftime("%H:%M:%S")

        # Set-Date 예: Set-Date -Date "05-20-2025 10:30:00"
        new_datetime_str = utc_datetime.strftime("%m-%d-%Y %H:%M:%S")

        # PowerShell 명령 실행
        subprocess.run([
            "powershell",
            "-Command",
            f"Set-Date -Date \"{new_datetime_str}\""
        ], check=True)

        print("윈도우 환경에서 바이낸스 서버 시간으로 동기화를 완료했습니다.")

    else:
        print("현재 OS가 macOS(또는 기타 POSIX 계열)인지 확인 후 macOS 분기로 넘어갑니다.")
        # 맥 등 나머지 OS 처리
        sync_binance_time_macos(utc_datetime)


def sync_binance_time_macos(utc_datetime):
    import subprocess

    # macOS 시스템 시간 설정: systemsetup 명령어
    # 날짜 설정 : sudo systemsetup -setdate MM:DD:YY
    # 시간 설정 : sudo systemsetup -settime HH:MM:SS
    # UTC 시간을 그대로 쓰면 macOS 로컬타임과 차이가 있을 수 있으니, 아래는 UTC 시간을 이용한 예시
    date_str = utc_datetime.strftime("%m:%d:%y")
    time_str = utc_datetime.strftime("%H:%M:%S")

    # 날짜 설정
    subprocess.run(["sudo", "systemsetup", "-setdate", date_str], check=True)
    # 시간 설정
    subprocess.run(["sudo", "systemsetup", "-settime", time_str], check=True)

    print("macOS 환경에서 바이낸스 서버 시간으로 동기화를 완료했습니다.")


if __name__ == "__main__":
    sync_binance_time()