import requests
from bs4 import BeautifulSoup

def crawl_investing_news():
    # 1) 크롤링 대상 URL (검색어: 'bitcoin', 탭: 'news')
    url = "https://www.investing.com/search/?q=bitcoin&tab=news"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                      " AppleWebKit/537.36 (KHTML, like Gecko)"
                      " Chrome/91.0.4472.124 Safari/537.36"
    }

    # 2) 메인 페이지 요청
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("페이지 로드 실패:", response.status_code)
        return

    # 3) BeautifulSoup 객체 생성
    soup = BeautifulSoup(response.text, "lxml")

    # 4) "data-tab-name" 이 "news" 인 div 영역 (뉴스 섹션) 찾기
    news_tab = soup.find("div", {"data-tab-name": "news"})
    if not news_tab:
        print("news 탭 영역을 찾지 못했습니다.")
        return

    # 5) 모든 기사 태그(div.articleItem) 추출
    article_items = news_tab.find_all("div", class_="articleItem")

    # 6) 기사 정보를 담을 리스트
    news_list = []

    for article in article_items:
        # 6-1) 헤더라인 (기사 제목)
        title_tag = article.find("a", class_="title")
        headline = title_tag.get_text(strip=True) if title_tag else "제목 없음"

        # 6-2) 출처 (span 태그 안의 텍스트가 보통 "By XXX" 형태)
        provider_tag = article.find("span")
        provider = provider_tag.get_text(strip=True) if provider_tag else "출처 정보 없음"

        # 6-3) 날짜 (time 태그)
        date_tag = article.find("time", class_="date")
        date = date_tag.get_text(strip=True) if date_tag else "날짜 정보 없음"

        # 6-4) 내용 (p 태그 중 class="js-news-item-content")
        content_tag = article.find("p", class_="js-news-item-content")
        content = content_tag.get_text(strip=True) if content_tag else "내용 없음"

        # 6-5) 기사 URL (href)
        #      investing.com 메인 도메인을 붙여야 접근 가능
        link = None
        if title_tag and title_tag.has_attr("href"):
            link = "https://www.investing.com" + title_tag["href"]

        news_list.append({
            "headline": headline,
            "provider": provider,
            "date": date,
            "content": content,
            "link": link
        })

    # 7) 추출한 기사 정보 출력
    for idx, news in enumerate(news_list, start=1):
        print(f"[{idx}]")
        print("헤더라인:", news["headline"])
        print("출처:", news["provider"])
        print("날짜:", news["date"])
        print("내용:", news["content"])
        print("URL:", news["link"])
        print("-" * 80)


if __name__ == "__main__":
    crawl_investing_news()