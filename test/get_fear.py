from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

# Selenium 웹드라이버 설정
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=chrome_options)
url = "https://www.ubcindex.com/feargreed"
driver.get(url)

# 페이지 로딩 대기
time.sleep(5)  # JavaScript 로딩 대기

# 날짜와 공포지수 추출
try:
    # XPath로 날짜와 공포지수 요소 찾기
    date_element = driver.find_element(By.XPATH, "/html/body/div[1]/div/div/div/div[2]/div/div/div[1]/section/div/div/div[1]/div/div[1]/small[1]")
    score_element = driver.find_element(By.XPATH, "/html/body/div[1]/div/div/div/div[2]/div/div/div[1]/section/div/div/div[1]/div/div[2]")

    # 데이터 추출
    date = date_element.text.strip()
    score = score_element.text.strip().split("/")[0].strip()  # "/ 100" 제거

    # 텍스트 파일로 저장
    with open("fear_greed_index.txt", "w", encoding="utf-8") as file:
        file.write(f"날짜: {date}\n")
        file.write(f"공포지수: {score}\n")

    print(f"공포지수 정보가 fear_greed_index.txt 파일에 저장되었습니다:\n날짜: {date}, 공포지수: {score}")
except Exception as e:
    print(f"데이터를 추출하는 중 오류가 발생했습니다: {e}")
finally:
    driver.quit()
