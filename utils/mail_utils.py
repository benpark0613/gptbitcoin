# gptbitcoin/utils/mail_utils.py
# 메일 발송 관련 유틸 함수 모듈

import os
import smtplib
import ssl
import certifi
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

load_dotenv()  # .env 파일 로딩 (GMAIL_ADDRESS, GMAIL_APP_PASSWORD, GMAIL_RECIPIENT)

def send_gmail(subject: str, body: str) -> None:
    """Gmail SMTP를 사용해 메일을 전송한다.

    Args:
        subject (str): 메일 제목.
        body (str): 메일 본문.
    """
    # .env에서 환경변수 로드
    gmail_address = os.getenv("GMAIL_ADDRESS")        # 보내는 Gmail 주소
    gmail_app_password = os.getenv("GMAIL_APP_PASSWORD")  # 16자리 앱 비밀번호
    recipient = os.getenv("GMAIL_RECIPIENT")               # 수신자(본인 메일)

    if not gmail_address or not gmail_app_password or not recipient:
        raise ValueError("메일 환경변수가 설정되지 않았습니다.")

    # MIME 구성
    message = MIMEMultipart("alternative")
    message["From"] = gmail_address
    message["To"] = recipient
    message["Subject"] = subject

    text_part = MIMEText(body, "plain")
    message.attach(text_part)

    # SSL 컨텍스트 (certifi 사용)
    context = ssl.create_default_context(cafile=certifi.where())

    try:
        # SMTP 서버에 SSL로 연결
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(gmail_address, gmail_app_password)
            server.sendmail(gmail_address, recipient, message.as_string())
    except Exception as e:
        # 메일 전송 실패 시
        print(f"[mail_utils] 메일 전송 실패: {e}")
        raise
