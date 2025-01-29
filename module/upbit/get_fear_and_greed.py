import csv
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def get_upbit_fear_greed_index():
    """
    ubcindex.com 사이트에서 공포·탐욕 지수를 가져와
    딕셔너리 형태로 반환합니다. (오늘, 어제, 일주일 전 등)
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # 1) Selenium 드라이버 실행
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://www.ubcindex.com/feargreed")
    time.sleep(5)  # 페이지 로딩 대기(네트워크 환경에 따라 조절)

    # 2) 오늘(Today) 지수 스코어 추출
    score_element = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/div/div/div/div[2]/div/div/div[1]/section/div/div/div[1]/div/div[2]"
    )
    today_score = score_element.text.strip().split("/")[0].strip()  # 예: "45" 형태

    # 3) 과거 지수(어제, 일주일 전, 1개월 전 등) 테이블 추출
    table_rows = driver.find_elements(By.CSS_SELECTOR, "table.historyTbl tbody tr")
    # 첫 번째 행: 헤더, 두 번째 행: 실제 값
    header_row = [td.text.strip() for td in table_rows[0].find_elements(By.TAG_NAME, "td")]
    data_row = [td.text.strip() for td in table_rows[1].find_elements(By.TAG_NAME, "td")]

    # 한글 → 영어 매핑
    header_map = {
        "어제": "Yesterday",
        "일주일전": "1WeekAgo",
        "1개월전": "1MonthAgo",
        "3개월전": "3MonthsAgo",
        "6개월전": "6MonthsAgo",
        "1년전": "1YearAgo"
    }

    # 순회할 키들
    target_keys = ["어제", "일주일전", "1개월전", "3개월전", "6개월전", "1년전"]

    # 4) 추출된 지수를 담을 딕셔너리
    fear_greed_data = {
        "Today": today_score
    }

    for key in target_keys:
        if key in header_row:
            idx = header_row.index(key)
            eng_header = header_map[key]  # "Yesterday", "1WeekAgo" 등
            fear_greed_data[eng_header] = data_row[idx]
        else:
            fear_greed_data[header_map.get(key, key)] = ""

    # 5) 드라이버 종료
    driver.quit()

    # 6) 원하는 형태로 반환
    # 예: {
    #   "Today": "45",
    #   "Yesterday": "40",
    #   "1WeekAgo": "55",
    #   ...
    # }
    return fear_greed_data


# 모듈 단독 실행 예시
if __name__ == "__main__":
    result = get_upbit_fear_greed_index()
    print("업비트 공포·탐욕 지수(Fear & Greed Index):")
    for k, v in result.items():
        print(f"{k}: {v}")