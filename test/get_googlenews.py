import logging
import json
import time
import re
from datetime import datetime, timedelta

# Selenium 관련
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_url(query, start=0, date_filter=None):
    """
    구글 뉴스 URL 생성 함수. date_filter가 있을 경우 qdr 옵션을 추가.
    """
    base_url = "https://www.google.com/search"
    params = {
        "q": query,
        "gl": "us",
        "tbm": "nws",
        "start": start
    }
    if date_filter:
        params["tbs"] = f"qdr:{date_filter}"
    query_string = "&".join([f"{key}={value}" for key, value in params.items()])
    return f"{base_url}?{query_string}"


def get_news_data_selenium(query, num_results=10, date_filter=None):
    """
    Selenium을 사용하여 여러 페이지에서 뉴스를 수집하고,
    기사 제목, 요약, 날짜, 소스를 추출하여 리스트 형태로 반환.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    collected_results = []
    start = 0

    try:
        while len(collected_results) < num_results:
            url = generate_url(query, start, date_filter=date_filter)
            driver.get(url)

            # 페이지 로딩 및 동적 요소 처리를 위해 잠시 대기
            time.sleep(2)

            # 기사를 담고 있는 영역 찾기
            articles = driver.find_elements(By.CSS_SELECTOR, "div.SoaBEf")
            if not articles:
                break

            for el in articles:
                try:
                    title_el = el.find_element(By.CSS_SELECTOR, "div.MBeuO")
                    snippet_el = el.find_element(By.CSS_SELECTOR, ".GI74Re")
                    date_el = el.find_element(By.CSS_SELECTOR, ".LfVVr")
                    source_el = el.find_element(By.CSS_SELECTOR, ".NUnG9d span")

                    title = title_el.text.strip() if title_el else "N/A"
                    snippet = snippet_el.text.strip() if snippet_el else "N/A"
                    date_str = date_el.text.strip() if date_el else "N/A"
                    source = source_el.text.strip() if source_el else "N/A"

                    collected_results.append({
                        "title": title,
                        "snippet": snippet,
                        "date": date_str,
                        "source": source
                    })
                except:
                    # 개별 기사 요소가 없거나 파싱 실패 시 넘어감
                    pass

            start += 10
            if len(articles) == 0:
                break

    finally:
        driver.quit()

    # 필요한 만큼만 잘라서 반환
    return collected_results[:num_results]


def test_google_news_scraping():
    """
    구글 뉴스 스크래핑 함수를 테스트하는 예시 함수.
    """
    # 예시로 'Bitcoin' 키워드로 최대 10개의 기사를 크롤링해보자
    keyword = "Bitcoin"
    num_results = 10

    logger.info(f"[TEST] '{keyword}' 키워드, 최대 {num_results}개 뉴스 기사 스크래핑을 시작합니다.")
    news_data = get_news_data_selenium(query=keyword, num_results=num_results)

    logger.info(f"[TEST] 스크래핑 완료. 총 {len(news_data)}개 기사.")

    # 스크래핑 결과 콘솔에 출력
    for idx, article in enumerate(news_data, start=1):
        print(f"\n[{idx}번 기사]")
        print(json.dumps(article, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    test_google_news_scraping()
