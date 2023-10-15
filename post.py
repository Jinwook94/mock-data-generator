from faker import Faker
import pandas as pd
import mysql.connector
import datetime
import numpy as np
import time
from prettytable import PrettyTable
from db_connector import get_db_connection

fake = Faker()

num_rows = 1000000

images = [
    '<p><img src="https://bootme-images.s3.ap-northeast-2.amazonaws.com/post/66/20230618_2137_20230618_213713.jpg"></p>',
    '<p><img src="https://bootme-images.s3.ap-northeast-2.amazonaws.com/post/57/20230612_2243_bird-wings-flying-feature.gif"></p>',
    '<p><img src="https://bootme-images.s3.ap-northeast-2.amazonaws.com/post/64/20230613_2348_스크린샷 2023-06-13 오후 11.48.24.png"></p>',
    '<p><img src="https://preview.redd.it/bq54d2yu1ss91.jpg?width=640&crop=smart&auto=webp&s=6cea8656c775dbea3375b12f9fa90b0ecb66f3d7"></p>',
    '<p><img src="https://preview.redd.it/g37eb3qivuv81.jpg?width=640&crop=smart&auto=webp&s=6ceadcfc725cc6f7c8836f052e912fdcb0a35d1e"></p>',
    '<p><img src="https://preview.redd.it/5cxgt5dhtzz91.png?width=640&crop=smart&auto=webp&s=2a63ce510c51bbf6c73fd3300144e2d9b7301b29"></p>',
    '<p><img src="https://preview.redd.it/hwurhp7crzf81.png?width=640&crop=smart&auto=webp&s=301899c42cdc435ed144aaf0a09ddea335c19866"></p>',
    '<p><img src="https://preview.redd.it/u5jwlxt1k43a1.jpg?width=640&crop=smart&auto=webp&s=3f4933374a613a4f952177fd683a5122d5cf3c8c"></p>',
    '<p><img src="https://preview.redd.it/jd25yqv8xsf31.jpg?width=640&crop=smart&auto=webp&s=9f146e09eed275511b156916db118ec9bb70a2da"></p>',
    '<iframe class="ql-video" frameborder="0" allowfullscreen="true" src="https://bootme-images.s3.ap-northeast-2.amazonaws.com/etc/merang.mp4"></iframe>',
    '<iframe class="ql-video" frameborder="0" allowfullscreen="true" src="https://www.youtube.com/embed/ADqLBc1vFwI?showinfo=0"></iframe>',
    '<iframe class="ql-video" frameborder="0" allowfullscreen="true" src="https://www.youtube.com/embed/TgJ2EKI7ZZw?showinfo=0"></iframe>'
]

time_dict = {
    'Mock data 생성': 0,
    'DB 연결': 0,
    '테이블 초기화': 0,
    'DB에 mock data 입력': 0
}

# 1. Mock data 생성
print("\n1. Mock data 생성")
mock_start_time = time.time()

start_time = datetime.datetime(2023, 7, 1)
time_increments = np.array(range(num_rows)) * 1
time_values = [start_time + datetime.timedelta(seconds=int(inc)) for inc in time_increments.tolist()]

data = {
    "member_id": [fake.random_int(min=1, max=4) for _ in range(num_rows)],
    "topic": [fake.random_element(elements=('자유', '개발 질문', '부트캠프 질문')) for _ in range(num_rows)],
    "title": [f'{i + 1}번째 게시글' for i in range(num_rows)],
    "content": ['<p>' + fake.text() + '</p>' + (np.random.choice(images) if np.random.rand() < 0.25 else '') for _ in
                range(num_rows)],  # 25% 확률로 이미지도 추가
    "likes": [fake.random_number(digits=5) for _ in range(num_rows)],
    "clicks": [fake.random_number(digits=4) for _ in range(num_rows)],
    "bookmarks": [fake.random_number(digits=3) for _ in range(num_rows)],
    "status": 'DISPLAY',
    "created_at": time_values,
    "modified_at": time_values,
}
df = pd.DataFrame(data)
mock_end_time = time.time()
time_dict['Mock data 생성'] = mock_end_time - mock_start_time

print(f"생성된 mock data 수: {num_rows}")
print(f"소요 시간: {mock_end_time - mock_start_time:.2f}초")

# 2. DB 연결
db_name = "bootme"
table_name = "post"

mydb = get_db_connection()

print("\n2. DB 연결")
if mydb.is_connected():
    db_info = mydb.get_server_info()
    print("연결된 RDBMS 버전: ", db_info)
    cursor = mydb.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("연결된 DB: ", record)
else:
    print("DB 연결 실패")

# 3. Post 테이블 초기화
table_start_time = time.time()
print("\n3. 테이블 초기화")
cursor = mydb.cursor()
try:
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")    # 외래키 제약 조건을 일시적으로 비활성화
    cursor.execute("TRUNCATE TABLE post_bookmark")
    cursor.execute("TRUNCATE TABLE post")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")     # 외래키 제약 조건 다시 활성화

    table_end_time = time.time()
    print("`post_bookmark`, `post` 테이블 초기화 완료")
    print("소요 시간: {:.2f}초".format(table_end_time - table_start_time))
    time_dict['테이블 초기화'] = table_end_time - table_start_time
except mysql.connector.Error as err:
    print(f"테이블 초기화 실패 : {err}")
    exit(1)


# 4. DB에 mock data 입력
batch_size = 5000 if num_rows >= 7000 else num_rows  # num_rows 값이 7000 이상이면 5000개씩 끊어서 입력

sql = f"INSERT INTO {table_name} (member_id, topic, title, content, likes, clicks, bookmarks, status, created_at, modified_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
start_time = time.time()
print("\n4. Mock Data 입력")
try:
    for i in range(0, len(df), batch_size):
        # batch 만큼 data를 가져와 list of tuple로 변환
        batch_data = [tuple(x) for x in df[i:i + batch_size].values]

        # execute_many()로 한번에 batch_size만큼 데이터 입력
        cursor.executemany(sql, batch_data)
        mydb.commit()

    end_time = time.time()
    elapsed_time = end_time - start_time
    time_dict['DB에 mock data 입력'] = end_time - start_time
    print("입력 대상: `{}` DB의 `{}` 테이블".format(db_name, table_name))
    print("입력된 mock data 수: {}".format(num_rows))
    print("소요 시간: {:.2f}초".format(elapsed_time))

except mysql.connector.Error as err:
    print("{} DB의 {} 테이블에 mock data 입력을 실패했습니다 : {}".format(db_name, table_name, err))
    mydb.rollback()

finally:
    if mydb.is_connected():
        cursor.close()
        mydb.close()
        print("\n5. 종료")
        print("DB 커넥션 종료\n")

# 5.소요시간 측정 표 출력
table = PrettyTable()
table.field_names = ["No.", "작업", "소요 시간 (초)"]

for i, (key, value) in enumerate(time_dict.items(), start=1):
    table.add_row([i, key, f"{value:.2f}"])

table.add_row(["-" * 4, "-" * 40, "-" * 9])
table.add_row(['합계', '', f"{end_time - mock_start_time:.2f}"])
print(table)

# TEST