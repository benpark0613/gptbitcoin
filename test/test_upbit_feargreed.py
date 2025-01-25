from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

# Selenium 웹드라이버 설정 (헤드리스 모드)
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=chrome_options)

url = "https://www.ubcindex.com/feargreed"
driver.get(url)

# 페이지 로딩 대기 (JS 로딩을 위해 약간의 대기)
time.sleep(5)

try:
    # 테이블의 행 추출
    table_rows = driver.find_elements(By.CSS_SELECTOR, "table.historyTbl tbody tr")

    # 첫 번째 행: 일시, 어제, 일주일전, ... (헤더)
    header_row = [td.text.strip() for td in table_rows[0].find_elements(By.TAG_NAME, "td")]

    # 두 번째 행: 점수, 59.15, 65.85, ... (우리가 원하는 점수 데이터)
    score_row = [td.text.strip() for td in table_rows[1].find_elements(By.TAG_NAME, "td")]

    # 파일에 기록
    with open("fear_greed_table_cleaned.txt", "w", encoding="utf-8") as file:
        file.write("\t".join(header_row) + "\n")
        file.write("\t".join(score_row) + "\n")

    print("필요한 행(일시, 점수)만 fear_greed_table_cleaned.txt 파일에 저장 완료.")
except Exception as e:
    print(f"테이블 데이터를 가져오는 중 오류가 발생했습니다: {e}")
finally:
    driver.quit()
