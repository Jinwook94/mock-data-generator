from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from decouple import config
from db_connector import get_db_connection

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

# 한 번에 처리할 레코드 수를 정의합니다. 여기서는 10,000으로 설정합니다.
batch_size = 10000  # Adjust the batch size as appropriate

try:
    # 총 레코드 수를 가져옵니다.
    cursor.execute("SELECT COUNT(*) FROM post")
    total_count = cursor.fetchone()['COUNT(*)']
    print(f"Total records to process: {total_count}")

    # 처리할 전체 페이지 수를 계산합니다.
    pages = total_count // batch_size + (1 if total_count % batch_size else 0)

    for page in range(pages):
        offset = page * batch_size

        # JOIN 쿼리를 사용하여 필요한 정보를 가져옵니다.
        query = """
        SELECT p.*, m.nickname AS writerNickname, m.profile_image AS writerProfileImage, 
               (SELECT COUNT(*) FROM comment c WHERE c.post_id = p.post_id) AS commentCount
        FROM post p
        JOIN member m ON p.member_id = m.member_id
        LIMIT %s OFFSET %s
        """
        cursor.execute(query, (batch_size, offset))
        rows = cursor.fetchall()

        # 각 레코드에 대한 액션을 생성합니다.
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

        # Bulk 인덱싱을 실행합니다.
        helpers.bulk(es, actions)
        print(f"Batch {page + 1}/{pages} indexed.")

except Exception as e:
    print(f"Error: {e}")

finally:
    if mydb.is_connected():
        cursor.close()
        mydb.close()
