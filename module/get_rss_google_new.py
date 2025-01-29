import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
import datetime
import pytz


def get_top_10_recent_news(rss_url):
    """
    주어진 RSS URL에서 기사 정보를 수집한 뒤,
    내림차순 정렬 기준 상위 10개의 (date, title) 정보를
    리스트(dict) 형태로 반환합니다.
    날짜는 한국 시간(KST)으로 변환됩니다.
    """
    response = requests.get(rss_url)
    root = ET.fromstring(response.content)

    korea_tz = pytz.timezone("Asia/Seoul")  # 한국 시간대(UTC+9)
    articles = []

    for item in root.findall(".//item"):
        pub_date_str = item.find("pubDate").text if item.find("pubDate") is not None else ""
        title_str = item.find("title").text if item.find("title") is not None else ""

        dt_utc = None
        if pub_date_str:
            try:
                dt_utc = parsedate_to_datetime(pub_date_str)
            except:
                pass  # 날짜 파싱 실패 시 그대로 None 유지

        # UTC -> KST 변환
        if dt_utc is not None:
            dt_kst = dt_utc.astimezone(korea_tz)
            pub_date_kst_str = dt_kst.strftime("%Y-%m-%d %H:%M:%S KST")
        else:
            pub_date_kst_str = pub_date_str

        articles.append({
            "date_utc": dt_utc,  # 정렬용 UTC datetime
            "date_kst_str": pub_date_kst_str,  # 사용자 확인용 KST 날짜 문자열
            "title": title_str
        })

    # 날짜(UTC) 기준 내림차순 정렬
    articles.sort(
        key=lambda x: x["date_utc"] if x["date_utc"] else datetime.datetime.min,
        reverse=True
    )

    # 최대 100개 중에서 상위 10개만
    top_10_articles = articles[:100][:10]

    # 반환할 리스트 구성 (date, title) 만 추출
    result = []
    for article in top_10_articles:
        result.append({
            "date": article["date_kst_str"],
            "title": article["title"]
        })

    return result


# 사용 예시 (직접 실행 시):
if __name__ == "__main__":
    rss_url = "https://news.google.com/rss/search?q=bitcoin&hl=en&gl=US"
    news_list = get_top_10_recent_news(rss_url)

    for idx, news in enumerate(news_list, 1):
        print(f"[{idx}] {news['date']} / {news['title']}")