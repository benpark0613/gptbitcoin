import logging
import json
import time
import re
import csv
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_relative_time_kor(time_str):
    """
    '14시간 전', '3분 전', '2일 전' 등 한국어 상대 시간 표기를
    datetime 객체로 변환 (대략적인 과거 시점).
    파싱 실패 시 None 반환.
    """
    time_str = time_str.strip()
    pattern = re.compile(r"(\d+)(일|시간|분)\s*전")
    match = pattern.match(time_str)
    if not match:
        return None
    value, unit = match.groups()
    value = int(value)

    now = datetime.now()
    if unit == '일':
        return now - timedelta(days=value)
    elif unit == '시간':
        return now - timedelta(hours=value)
    elif unit == '분':
        return now - timedelta(minutes=value)
    else:
        return None

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

def get_latest_10_articles(query, date_filter=None):
    """
    - 최대 30개의 기사를 스크래핑
    - 날짜(상대시간) 파싱 후 '가장 최근 기사' 순으로 정렬
    - 상위 10개 기사만 반환 (CSV 저장은 하지 않음)
    """
    # 크롬 옵션
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/110.0.5481.77 Safari/537.36"
    )

    driver = webdriver.Chrome(options=chrome_options)

    collected_results = []
    start = 0
    SCRAPE_TOTAL = 30  # 최대 30개 기사

    try:
        while len(collected_results) < SCRAPE_TOTAL:
            url = generate_url(query, start, date_filter=date_filter)
            driver.get(url)

            time.sleep(2)  # 필요 시 WebDriverWait으로 개선 가능

            articles = driver.find_elements(By.CSS_SELECTOR, "div.SoaBEf")
            if not articles:
                logger.info("더 이상 기사가 없습니다. 수집 중단.")
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

                    parsed_time = parse_relative_time_kor(date_str)

                    collected_results.append({
                        "title": title,
                        "snippet": snippet,
                        "date": date_str,
                        "source": source,
                        "parsed_date": parsed_time
                    })
                except Exception as e:
                    logger.warning(f"기사 파싱 중 에러: {e}")

            logger.info(
                f"이번 페이지 기사: {len(articles)}개, "
                f"누적 기사: {len(collected_results)}/{SCRAPE_TOTAL}"
            )
            start += 10

    finally:
        driver.quit()

    logger.info(f"총 수집 기사: {len(collected_results)}개")

    # 기사 날짜(parsed_date) 기준 내림차순 정렬 (None이면 datetime.min)
    collected_results.sort(
        key=lambda x: x["parsed_date"] if x["parsed_date"] else datetime.min,
        reverse=True
    )

    # 상위 10개 기사만 추출
    top_10 = collected_results[:10]
    logger.info(f"최신 기사 10개(또는 그 미만) 추출 완료. 실제 개수: {len(top_10)}")

    return top_10

def test_google_news_scraping():
    """
    테스트 함수:
    1) 'Bitcoin' 키워드로 get_latest_10_articles() 호출하여 10개 기사만 받음
    2) 받은 기사들을 CSV 저장 (테스트 용)
    """
    keyword = "Bitcoin"
    logger.info(f"[TEST] '{keyword}' 키워드로 최신 기사 10개만 크롤링합니다.")

    news_data = get_latest_10_articles(query=keyword)
    logger.info(f"최종 반환된 기사: {len(news_data)}개")

    # -- CSV 저장은 '외부에서' 처리 (테스트 용) --
    csv_filename = "test_10_latest_articles.csv"
    try:
        with open(csv_filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["title", "snippet", "date", "source", "parsed_date"])
            for article in news_data:
                writer.writerow([
                    article["title"],
                    article["snippet"],
                    article["date"],
                    article["source"],
                    str(article["parsed_date"]) if article["parsed_date"] else ""
                ])
        logger.info(f"테스트 용으로 {len(news_data)}개 기사 CSV 저장 완료: {csv_filename}")
    except Exception as e:
        logger.error(f"CSV 저장 에러: {e}")

    # 콘솔 출력
    for idx, article in enumerate(news_data, start=1):
        print(f"\n[{idx}번 기사 - parsed_date={article['parsed_date']}]")
        print(json.dumps(article, indent=4, ensure_ascii=False, default=str))

if __name__ == "__main__":
    test_google_news_scraping()
