from faker import Faker
import pandas as pd
import mysql.connector
import numpy as np
import datetime
import mysql.connector
import time
from prettytable import PrettyTable
from db_connector import get_db_connection


# 소요시간 측정 위한 dict
time_dict = {
    'DB 연결': 0,
    'post 테이블 행 개수 조회': 0,
    'Comment mock data 생성': 0,
    'Comment 테이블 초기화': 0,
    'DB에 Comment mock data 입력': 0
}

# 1. DB 연결
print("\n1. DB 연결")
start_time = time.time()

db_name = "bootme"
table_name = "comment"

mydb = get_db_connection()

if mydb.is_connected():
    db_info = mydb.get_server_info()
    print("연결된 RDBMS 버전: ", db_info)
    cursor = mydb.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("연결된 DB: ", record)
else:
    print("DB 연결 실패")

cursor = mydb.cursor()
end_time = time.time()
time_dict['DB 연결'] = end_time - start_time

# 2. post 테이블 행 수 가져오기
print("\n2. post 테이블의 행 개수 가져오기")
start_time = time.time()
cursor.execute("SELECT COUNT(*) FROM post")
num_posts = cursor.fetchone()[0]
print("post 테이블 행 수:", num_posts)
end_time = time.time()
time_dict['post 테이블 행 개수 조회'] = end_time - start_time

# 3. Comment mock data 생성
print("\n3. Comment mock data 생성")
mock_start_time = time.time()

fake = Faker()
num_rows = num_posts * 10  # 각 Post당 10개의 Comment

comment_start_time = datetime.datetime(2023, 7, 1)
comment_time_increments = np.array(range(num_rows)) * 1
comment_time_values = [comment_start_time + datetime.timedelta(seconds=int(inc)) for inc in comment_time_increments.tolist()]

comment_data = {
    "post_id": [],
    "member_id": [],
    "content": [],
    "group_num": [],
    "level_num": [],
    "order_num": [],
    "likes": [],
    "status": 'DISPLAY',
    "created_at": [],
    "modified_at": []
}

# 진행 상황 출력
progress_interval = num_rows // 10
for i in range(num_rows):
    comment_data["post_id"].append(i % num_posts + 1)
    comment_data["member_id"].append(fake.random_int(min=1, max=4))
    comment_data["content"].append('<p>' + fake.text() + '</p>')
    comment_data["group_num"].append(1)
    comment_data["level_num"].append(0)
    comment_data["order_num"].append(0)
    comment_data["likes"].append(fake.random_number(digits=4))
    comment_data["created_at"].append(comment_time_values[i])
    comment_data["modified_at"].append(comment_time_values[i])

    if i > 0 and i % progress_interval == 0:
        progress = i * 100 / num_rows
        print(f"진행 상황: {progress}% 완료")

comment_df = pd.DataFrame(comment_data)
mock_end_time = time.time()
time_dict['Comment mock data 생성'] = mock_end_time - mock_start_time

print(f"생성된 Comment mock data 수: {num_rows}")
print(f"소요 시간: {mock_end_time - mock_start_time:.2f}초")


# 4. Comment 테이블 초기화
print("\n4. Comment 테이블 초기화")
start_time = time.time()
cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
cursor.execute("TRUNCATE TABLE comment")
cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
end_time = time.time()
time_dict['Comment 테이블 초기화'] = end_time - start_time
print("`comment` 테이블 초기화 완료")
print("소요 시간: {:.2f}초".format(end_time - start_time))

# 5. DB에 Comment mock data 입력
print("\n5. Mock Data 입력")
start_time = time.time()

comment_batch_size = 5000 if num_rows >= 7000 else num_rows

comment_sql = f"INSERT INTO {table_name} (post_id, member_id, content, group_num, level_num, order_num, likes, status, created_at, modified_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
try:
    total_batches = len(comment_df) // comment_batch_size
    if len(comment_df) % comment_batch_size != 0:
        total_batches += 1

    for i in range(0, len(comment_df), comment_batch_size):
        # Comment mock data의 batch 단위로 DB에 입력
        comment_batch_data = [tuple(x) for x in comment_df[i:i + comment_batch_size].values]
        cursor.executemany(comment_sql, comment_batch_data)
        mydb.commit()

        # 진행상황 출력
        if total_batches < 10 or (i // comment_batch_size + 1) % max(total_batches // 10, 1) == 0:
            progress = (i // comment_batch_size + 1) * 100 / total_batches
            print(f"진행 상황: {progress}% 완료")

    end_time = time.time()
    time_dict['DB에 Comment mock data 입력'] = end_time - start_time
    print("입력 대상: `{}` DB의 `{}` 테이블".format(db_name, table_name))
    print("입력된 mock data 수: {}".format(num_rows))
    print("소요 시간: {:.2f}초".format(end_time - start_time))

except mysql.connector.Error as err:
    print("{} DB의 {} 테이블에 mock data 입력을 실패했습니다 : {}".format(db_name, table_name, err))
    mydb.rollback()

finally:
    if mydb.is_connected():
        cursor.close()
        mydb.close()
        print("\n5. 종료")
        print("DB 커넥션 종료\n")

# 6. 소요시간 측정 표 출력
table = PrettyTable()
table.field_names = ["No.", "작업", "소요 시간 (초)"]

for i, (key, value) in enumerate(time_dict.items(), start=1):
    table.add_row([i, key, f"{value:.2f}"])

table.add_row(["-" * 4, "-" * 40, "-" * 9])
table.add_row(['합계', '', f"{sum(time_dict.values()):.2f}"])
print(table)
