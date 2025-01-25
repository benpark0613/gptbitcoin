from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import csv

# 1. Selenium 웹드라이버 설정
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=chrome_options)

try:
    url = "https://www.ubcindex.com/feargreed"
    driver.get(url)

    # 2. 페이지 로딩 대기 (JS 렌더링)
    time.sleep(5)

    # 3. 상단 “오늘(Today)” 공포지수(FearIndex) 추출
    score_element = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/div/div/div/div[2]/div/div/div[1]/section/div/div/div[1]/div/div[2]"
    )
    today_score = score_element.text.strip().split("/")[0].strip()

    # 4. 테이블 행 추출
    table_rows = driver.find_elements(By.CSS_SELECTOR, "table.historyTbl tbody tr")

    # 첫 번째 행(헤더)
    header_row = [td.text.strip() for td in table_rows[0].find_elements(By.TAG_NAME, "td")]
    # 두 번째 행(데이터)
    data_row = [td.text.strip() for td in table_rows[1].find_elements(By.TAG_NAME, "td")]

    # 5. 한국어 항목을 영어로 변환하기 위한 매핑(mapping)
    #    한국어 헤더를 보고, 해당 위치의 데이터도 함께 영어 표현으로 변환
    #    예: "어제" → "Yesterday", "일주일전" → "OneWeekAgo"
    header_map = {
        "어제": "Yesterday",
        "일주일전": "1WeekAgo",
        "1개월전": "1MonthAgo",
        "3개월전": "3MonthsAgo",
        "6개월전": "6MonthsAgo",
        "1년전": "1YearAgo"
    }

    # 6. 추출할 대상 키 목록
    target_keys = ["어제", "일주일전", "1개월전", "3개월전", "6개월전", "1년전"]

    extracted_data = []
    for key in target_keys:
        if key in header_row:
            idx = header_row.index(key)
            # 영어로 변환한 헤더, 해당하는 값
            eng_header = header_map.get(key, key)
            value = data_row[idx]
            extracted_data.append((eng_header, value))
        else:
            # 표에 해당 키가 없다면, 빈 값 또는 0
            eng_header = header_map.get(key, key)
            extracted_data.append((eng_header, ""))

    # 7. CSV로 저장
    # 첫 줄: Time, FearIndex
    # 두 번째 줄: Today, today_score
    # 이후: (Yesterday, ~), (OneWeekAgo, ~), ...
    with open("fear_greed_english.csv", mode="w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        # 헤더
        writer.writerow(["Time", "FearIndex"])

        # 오늘(Today)
        writer.writerow(["Today", today_score])

        # 나머지 항목들
        for eng_header, value in extracted_data:
            writer.writerow([eng_header, value])

    print("CSV 파일(fear_greed_english.csv) 생성 완료.")

except Exception as e:
    print(f"데이터 추출 중 오류가 발생했습니다: {e}")
finally:
    driver.quit()
