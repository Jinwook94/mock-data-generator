from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from decouple import config
from db_connector import get_db_connection
import time
from prettytable import PrettyTable

# Elasticsearch 클라이언트 설정
es = Elasticsearch(
    [{
        'host': config('ES_HOST'),
        'port': config('ES_PORT', default=9200, cast=int),
        'scheme': "http",
    }]
)

# MariaDB 연결
mydb = get_db_connection()
cursor = mydb.cursor(dictionary=True)

# Elasticsearch 인덱스명
es_index = 'post'

# 한 번에 처리할 레코드 수를 정의
batch_size = 10000

# 작업 시간 측정 위한 딕셔너리
time_dict = {
    'Elasticsearch에 데이터 인덱싱': 0,
}

try:
    # 총 레코드 수 가져옴
    cursor.execute("SELECT COUNT(*) FROM post")
    total_count = cursor.fetchone()['COUNT(*)']
    print(f"Total records to process: {total_count}")

    # 처리할 전체 페이지 수를 계산
    pages = total_count // batch_size + (1 if total_count % batch_size else 0)

    indexing_start_time = time.time()

    for page in range(pages):
        offset = page * batch_size

        # JOIN 쿼리를 사용하여 필요한 정보를 가져옴
        query = """
        SELECT p.*, m.nickname AS writerNickname, m.profile_image AS writerProfileImage, 
               (SELECT COUNT(*) FROM comment c WHERE c.post_id = p.post_id) AS commentCount
        FROM post p
        JOIN member m ON p.member_id = m.member_id
        LIMIT %s OFFSET %s
        """
        cursor.execute(query, (batch_size, offset))
        rows = cursor.fetchall()

        # 각 레코드에 대한 액션을 생성
        actions = [
            {
                "_index": es_index,
                "_source": {
                    'postId': row['post_id'],
                    'memberId': row['member_id'],
                    'writerNickname': row['writerNickname'],
                    'writerProfileImage': row['writerProfileImage'],
                    'commentCount': row['commentCount'],
                    'topic': row['topic'],
                    'title': row['title'],
                    'content': row['content'],
                    'likes': row['likes'],
                    'clicks': row['clicks'],
                    'bookmarks': row['bookmarks'],
                    'status': row['status'],
                    'createdAt': int(row['created_at'].timestamp() * 1000),
                    'modifiedAt': int(row['modified_at'].timestamp() * 1000),
                    '@timestamp': datetime.utcnow().isoformat()
                }
            }
            for row in rows
        ]

        # Bulk 인덱싱 실행
        helpers.bulk(es, actions)
        print(f"Batch {page + 1}/{pages} indexed.")

        indexing_end_time = time.time()
        time_dict['Elasticsearch에 데이터 인덱싱'] = indexing_end_time - indexing_start_time

except Exception as e:
    print(f"Error: {e}")

finally:
    if mydb.is_connected():
        cursor.close()
        mydb.close()

    # 총 소요 시간을 계산하여 추가
    total_time = sum(time_dict.values())
    time_dict['Total'] = total_time

    # 소요시간 측정 표 출력
    table = PrettyTable()
    table.field_names = ["작업", "소요 시간 (초)"]
    for key, value in time_dict.items():
        table.add_row([key, f"{value:.2f}"])

    print(table)
