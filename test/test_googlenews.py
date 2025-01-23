import csv
import logging
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============== 폴더 생성 함수 ==============
def create_folders(base_folder_name="test_report"):
    """
    기본 베이스 폴더(trade_report) / 날짜 폴더 / 시간 폴더 구조를 생성하고
    최종적으로 저장할 time_folder 경로를 반환
    """
    base_folder = base_folder_name
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    today_date = datetime.now().strftime("%Y%m%d")
    date_folder = os.path.join(base_folder, today_date)
    if not os.path.exists(date_folder):
        os.makedirs(date_folder)

    current_time = datetime.now().strftime("%H%M%S")
    time_folder = os.path.join(date_folder, current_time)
    if not os.path.exists(time_folder):
        os.makedirs(time_folder)

    return time_folder

# ============== 구글 뉴스 크롤링 관련 함수 ==============
def generate_url(query, start=0, date_filter='m'):
    """
    Google 뉴스 검색 URL 생성 함수.
    date_filter='m'일 경우 지난 1개월, 'w'는 1주, 'd'는 24시간.
    """
    base_url = "https://www.google.com/search"
    params = {
        "q": query,
        "gl": "us",     # 미국 기반 검색
        "tbm": "nws",   # 뉴스 검색
        "start": start,
        "tbs": f"qdr:{date_filter}"
    }
    return f"{base_url}?{'&'.join([f'{key}={value}' for key, value in params.items()])}"


def get_news_on_page(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    articles = soup.select("div.SoaBEf")
    page_results = []
    for el in articles:
        link_el = el.find("a")
        link = link_el.get("href", "") if link_el else "N/A"

        title_el = el.select_one("div.MBeuO")
        title = title_el.get_text(strip=True) if title_el else "N/A"

        snippet_el = el.select_one(".GI74Re")
        snippet = snippet_el.get_text(strip=True) if snippet_el else "N/A"

        date_el = el.select_one(".LfVVr")
        date = date_el.get_text(strip=True) if date_el else "N/A"

        source_el = el.select_one(".NUnG9d span")
        source = source_el.get_text(strip=True) if source_el else "N/A"

        page_results.append({
            "link": link,
            "title": title,
            "snippet": snippet,
            "date": date,
            "source": source
        })
    return page_results


def get_news_data(query, num_results=10, date_filter='m'):
    """
    최대 num_results개를 모을 때까지 페이지를 넘겨가며 뉴스를 수집.
    지난 date_filter 기간을 제한(qdr:m => 1개월).
    """
    collected_results = []
    start = 0

    while len(collected_results) < num_results:
        url = generate_url(query, start, date_filter=date_filter)
        page_data = get_news_on_page(url)
        if not page_data:
            # 더 이상 결과가 없으면 중단
            break
        collected_results.extend(page_data)
        start += 10  # 다음 페이지로 이동

    return collected_results[:num_results]


def save_news_to_csv(data, folder_path):
    """
    구글 뉴스 데이터를 CSV로 저장
    """
    csv_path = os.path.join(folder_path, "google_news.csv")
    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["title", "link", "snippet", "date", "source"]
        )
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Google News data saved to {csv_path}")

# ============== 메인 실행부 ==============
if __name__ == "__main__":
    output_folder = create_folders()

    # 구글 뉴스 크롤링
    query = "BTC OR Bitcoin"
    num_results = 10
    date_filter = 'w'  # 'w' => 지난 1주간
    news_data = get_news_data(query, num_results, date_filter)
    save_news_to_csv(news_data, output_folder)