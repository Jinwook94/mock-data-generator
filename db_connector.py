import mysql.connector
from decouple import config


def get_db_connection():
    try:
        db_name = config('DB_NAME')
        mydb = mysql.connector.connect(
            host=config('DB_HOST'),
            port=config('DB_PORT'),
            user=config('DB_USER'),
            password=config('DB_PASSWORD'),
            database=db_name
        )
        print("DB 연결 완료")
        return mydb
    except Exception as e:
        print(f"DB 연결 실패 : {e}")
        return None
