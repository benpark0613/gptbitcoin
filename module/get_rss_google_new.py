import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
import datetime
import pytz

def get_top_10_recent_news(rss_url):
    """
    주어진 RSS URL에서 기사 정보를 수집한 뒤,
    내림차순 정렬 기준 상위 10개의 (date, title, source) 정보를
    리스트(dict) 형태로 반환합니다.
    날짜는 한국 시간(KST)으로 변환됩니다.
    """
    response = requests.get(rss_url)
    root = ET.fromstring(response.content)

    korea_tz = pytz.timezone("Asia/Seoul")  # 한국 시간대(UTC+9)
    articles = []

    for item in root.findall(".//item"):
        pub_date_str = item.find("pubDate").text or ""
        raw_title_str = item.find("title").text or ""

        # pub_date 파싱 시도
        dt_utc = None
        if pub_date_str:
            try:
                dt_utc = parsedate_to_datetime(pub_date_str)
            except:
                pass  # 날짜 파싱 실패 시 dt_utc=None 유지

        # UTC -> KST 변환
        if dt_utc is not None:
            dt_kst = dt_utc.astimezone(korea_tz)
            pub_date_kst_str = dt_kst.strftime("%Y-%m-%d %H:%M:%S KST")
        else:
            pub_date_kst_str = pub_date_str

        # 제목에서 " - " 구분자 기준, 출처 분리
        # 예: "Countdown ... Purchase - TradingView"
        #     -> title="Countdown ... Purchase", source="TradingView"
        if " - " in raw_title_str:
            # 오른쪽에서 한 번만 나눔
            title_part, source_part = raw_title_str.rsplit(" - ", 1)
        else:
            title_part = raw_title_str
            source_part = ""

        articles.append({
            "date_utc": dt_utc,                # 정렬용 UTC datetime
            "date": pub_date_kst_str,          # 결과로 쓸 KST 날짜 문자열
            "title": title_part,
            "source": source_part
        })

    # 날짜(UTC) 기준 내림차순 정렬
    articles.sort(
        key=lambda x: x["date_utc"] if x["date_utc"] else datetime.datetime.min,
        reverse=True
    )

    # 최대 100개 중에서 상위 10개만
    top_10_articles = articles[:100][:10]

    # 반환할 리스트 (date, title, source)
    # date만 KST 문자열 사용
    result = []
    for article in top_10_articles:
        result.append({
            "date": article["date"],
            "title": article["title"],
            "source": article["source"]
        })

    return result

if __name__ == "__main__":
    rss_url = "https://news.google.com/rss/search?q=bitcoin&hl=en&gl=US"
    news_list = get_top_10_recent_news(rss_url)

    # CSV 저장 예시
    import pandas as pd
    import csv

    df = pd.DataFrame(news_list)

    # 큰따옴표 제거(quoting=csv.QUOTE_NONE), 구분자 기본(쉼표)
    df.to_csv("news_no_quotes.csv", index=False, quoting=csv.QUOTE_NONE, escapechar="\\")

    # 결과 출력
    for idx, news in enumerate(news_list, 1):
        print(f"[{idx}] {news['date']} / {news['title']} / {news['source']}")