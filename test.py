import os
import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime

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
        # tbs=qdr:m  => 지난 한 달
        # tbs=qdr:w  => 지난 한 주
        # tbs=qdr:d  => 지난 24시간
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

def create_folders():
    base_folder = "trade_report"
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

def save_to_csv(data, folder_path):
    csv_path = os.path.join(folder_path, "news_results.csv")
    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["title", "link", "snippet", "date", "source"]
        )
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    query = "BTC OR Bitcoin"
    num_results = 10
    date_filter = 'w'  # 'm' => 지난 1개월

    # 기간을 제한하여 뉴스 데이터 크롤링
    news_data = get_news_data(query, num_results, date_filter)

    # 폴더 생성
    output_folder = create_folders()

    # CSV 저장
    save_to_csv(news_data, output_folder)
    print(f"크롤링 데이터가 {output_folder}에 저장되었습니다.")