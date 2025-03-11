# gptbitcoin/test/kakao_messenger_test.py
# 카카오톡 '나에게 메시지 보내기' API를 테스트하는 스크립트

import requests

def send_kakao_to_me(access_token: str, message: str) -> bool:
    """카카오톡 '나에게 메시지 보내기' API로 메시지를 전송한다.

    Args:
        access_token (str): 카카오 REST API Access Token (사용자 인증 후 발급받은 값)
        message (str): 전송할 메시지

    Returns:
        bool: 전송 성공 시 True, 실패 시 False
    """
    # 카카오톡 '나에게 보내기' API 엔드포인트
    url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"

    # 요청 헤더 설정
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # 텍스트 템플릿
    template_object = {
        "object_type": "text",
        "text": message,
        "link": {
            "web_url": "https://example.com"
        }
    }

    # 요청 바디 구성
    data = {
        "template_object": str(template_object)
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            print("[카카오톡] 메시지 전송 성공!")
            return True
        else:
            print(f"[카카오톡] 메시지 전송 실패: {response.text}")
            return False
    except Exception as e:
        print(f"[카카오톡] 전송 중 예외 발생: {e}")
        return False

def main():
    """카카오톡 '나에게 메시지 보내기' 테스트 진입점."""
    # 사전에 발급받은 Access Token을 여기에 넣거나, 별도 config에서 불러온다.
    access_token = "YOUR_ACCESS_TOKEN_HERE"
    test_message = "포지션 변경 테스트 메시지"

    # 메시지 전송 함수 호출
    send_kakao_to_me(access_token, test_message)

if __name__ == "__main__":
    # 직접 실행 시 테스트 진행
    main()
