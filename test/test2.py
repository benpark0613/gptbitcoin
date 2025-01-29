import numpy as np
import matplotlib.pyplot as plt

# (1) 예시용 데이터 생성: 더블탑 구조를 갖도록 의도적으로 설계
#    - 처음에 가격이 상승하다가 (첫 번째 고점)
#    - 약간 조정 후 다시 비슷한 수준으로 상승 (두 번째 고점)
#    - 그 다음 큰 하락이 나타나는 형태

x = np.arange(1, 31)  # x축(시간)을 1~30으로 설정
y_list = []
# 구간별로 나누어 선형보간(Linear Interpolation)으로 y값 생성
# 1) 100 -> 200 (급상승, 첫 번째 고점)
y_list.extend(np.linspace(100, 200, 10))   # x=1~10
# 2) 200 -> 180 (조정)
y_list.extend(np.linspace(200, 180, 5))    # x=11~15
# 3) 180 -> 200 (다시 상승, 두 번째 고점)
y_list.extend(np.linspace(180, 200, 5))    # x=16~20
# 4) 200 -> 120 (큰 하락)
y_list.extend(np.linspace(200, 120, 10))   # x=21~30

y = np.array(y_list)

# (2) 차트 그리기
plt.figure(figsize=(10, 6))
plt.plot(x, y, marker='o', label='Price')

# (3) 두 개의 고점(Top)을 강조 표시
top1_x = 10
top1_y = y[top1_x - 1]  # 인덱스 보정(파이썬은 0부터 시작)
plt.plot(top1_x, top1_y, 'ro')
plt.annotate('First Top',
             xy=(top1_x, top1_y),
             xytext=(top1_x, top1_y+10),
             arrowprops=dict(facecolor='red', shrink=0.05),
             ha='center')

top2_x = 20
top2_y = y[top2_x - 1]
plt.plot(top2_x, top2_y, 'ro')
plt.annotate('Second Top',
             xy=(top2_x, top2_y),
             xytext=(top2_x, top2_y+10),
             arrowprops=dict(facecolor='red', shrink=0.05),
             ha='center')

# (4) 넥라인(Neckline) 부근 표시: 여기서는 두 고점 사이의 저점 수준을 가정(대략 y=180 근처)
neckline_y = 180
plt.hlines(neckline_y, 10, 20, colors='gray', linestyles='dashed', label='Neckline')

# (5) 그래프 꾸미기
plt.title('Double Top Pattern Example')
plt.xlabel('Time')
plt.ylabel('Price')
plt.legend()
plt.grid(True)
plt.show()