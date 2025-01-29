from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Chrome 드라이버 설정 및 브라우저 열기
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

try:
    # 바이낸스 비트코인 뉴스 페이지로 이동
    driver.get("https://www.binance.com/en/square/news/bitcoin%20news")

    # 페이지 로딩 대기 (필요시 조정)
    time.sleep(5)

    # 예시: 뉴스 카드 전체를 선택 (클래스명은 실제 페이지에 맞춰 조정 필요)
    # div 태그 중 css-9vunkz 등을 기준으로 잡음
    news_cards = driver.find_elements(By.CSS_SELECTOR, "div.css-9vunkz")

    # 각 뉴스 카드에서 필요한 정보 추출
    for card in news_cards:
        # 헤드라인(또는 제목)에 해당하는 부분
        headline_element = card.find_element(By.CSS_SELECTOR, "div.css-1ltrr70")  # 예시
        headline_text = headline_element.text if headline_element else "헤드라인 정보 없음"

        # 시간 정보
        time_element = card.find_element(By.CSS_SELECTOR, "div.css-vyakl8")  # 예시
        time_text = time_element.text if time_element else "시간 정보 없음"

        # 뉴스 본문 또는 내용
        # 실제 구조에 따라 아래 CSS_SELECTOR 부분을 변경해야 함
        # 기사 요약 부분을 표기한 클래스를 찾아서 넣어야 함
        try:
            content_element = card.find_element(By.CSS_SELECTOR, "div.css-1mrt2u7")
            content_text = content_element.text
        except:
            content_text = "본문 정보 없음"

        # 추출한 정보 출력
        print("===========")
        print(f"헤드라인: {headline_text}")
        print(f"시간: {time_text}")
        print(f"내용: {content_text}")

finally:
    # 브라우저 닫기 (테스트 후 유지 여부 결정)
    driver.quit()
