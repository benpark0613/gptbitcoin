import requests
import xml.etree.ElementTree as ET
import csv
from email.utils import parsedate_to_datetime
import datetime
import pytz


def save_top_10_recent_news(rss_url, csv_filename):
    response = requests.get(rss_url)
    root = ET.fromstring(response.content)

    korea_tz = pytz.timezone("Asia/Seoul")  # 한국 시간대(UTC+9)

    # 1. RSS 기사 정보 수집
    articles = []
    for item in root.findall(".//item"):
        # pubDate 문자열
        pub_date_str = item.find("pubDate").text if item.find("pubDate") is not None else ""
        # title 문자열
        title_str = item.find("title").text if item.find("title") is not None else ""

        # pubDate를 datetime(UTC)으로 변환
        dt_utc = None
        if pub_date_str:
            try:
                dt_utc = parsedate_to_datetime(pub_date_str)
            except:
                pass  # 파싱 실패 시 None 유지

        # UTC datetime을 KST로 변환
        if dt_utc is not None:
            dt_kst = dt_utc.astimezone(korea_tz)
            # 예) "2025-01-28 00:10:56 KST" 형태
            pub_date_kst_str = dt_kst.strftime("%Y-%m-%d %H:%M:%S KST")
        else:
            pub_date_kst_str = pub_date_str  # 변환 실패 시 원문 그대로

        # 리스트에 저장
        articles.append({
            "date_utc": dt_utc,  # 정렬용 (UTC) datetime
            "date_kst_str": pub_date_kst_str,  # CSV 기록용 한국 시간
            "title": title_str
        })

    # 2. UTC 날짜로 내림차순 정렬 (최근이 위로)
    #    dt_utc가 None이면 오래된 것으로 취급
    articles.sort(
        key=lambda x: x["date_utc"] if x["date_utc"] is not None else datetime.datetime.min,
        reverse=True
    )

    # 3. 최대 100개까지만 추려내기
    top_100_articles = articles[:100]

    # 4. 그중에서 최신 10개만 선별
    top_10_articles = top_100_articles[:10]

    # 5. CSV 저장
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        fieldnames = ["date", "title"]  # 열 제목(영어)
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for article in top_10_articles:
            writer.writerow({
                "date": article["date_kst_str"],
                "title": article["title"]
            })


if __name__ == "__main__":
    rss_url = "https://news.google.com/rss/search?q=bitcoin&hl=en&gl=US"
    csv_filename = "news_data.csv"

    save_top_10_recent_news(rss_url, csv_filename)
    print("CSV 파일 저장이 완료되었습니다. (최신 10개 뉴스, KST)")