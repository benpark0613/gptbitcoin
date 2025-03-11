import os
import smtplib
import ssl
import certifi
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_gmail(
    gmail_address: str,       # 보내는 Gmail 주소
    gmail_app_password: str,  # 앱 비밀번호(16자리)
    recipient: str,           # 받는 사람 이메일(=본인)
    subject: str,             # 메일 제목
    body: str                 # 메일 내용
):
    """앱 비밀번호를 이용해 Gmail에서 메일을 전송한다."""
    # 1) 메일(MIME) 구성
    message = MIMEMultipart("alternative")
    message["From"] = gmail_address
    message["To"] = recipient
    message["Subject"] = subject

    text_part = MIMEText(body, "plain")  # 본문(텍스트)
    message.attach(text_part)

    # 2) SSL 컨텍스트 (certifi의 CA 인증서 경로를 지정)
    context = ssl.create_default_context(cafile=certifi.where())

    # 3) SMTP 서버에 SSL로 연결
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        # Gmail 로그인 (앱 비밀번호 사용)
        server.login(gmail_address, gmail_app_password)
        # 4) 메일 전송
        server.sendmail(
            from_addr=gmail_address,
            to_addrs=recipient,
            msg=message.as_string()
        )
        print("[GMAIL] 메일 전송 완료")

if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("GMAIL_APP_PASSWORD")

    # ① 내 Gmail 주소, ② 앱 비밀번호(16자리), ③ 수신자=본인, ④ 제목, ⑤ 내용
    GMAIL_ADDRESS = "sg2pooh@gmail.com"
    GMAIL_APP_PASSWORD = api_key  # 발급받은 16자리
    RECIPIENT = "sg2pooh@gmail.com"           # 본인 이메일
    SUBJECT = "파이썬 SMTP 테스트"
    BODY = "이 메일은 파이썬 코드로 전송되었습니다."

    send_gmail(GMAIL_ADDRESS, GMAIL_APP_PASSWORD, RECIPIENT, SUBJECT, BODY)
