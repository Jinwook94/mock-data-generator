import mysql.connector
from decouple import config


def get_db_connection():
    try:
        db_name = config('DB_NAME')
        mydb = mysql.connector.connect(
            host=config('DB_HOST'),
            port=config('DB_PORT'),
            user=config('DB_USER'),
            password=config('DB_password'),
            database=db_name
        )
        return mydb
    except Exception as e:
        print(f"DB 연결 실패 : {e}")
        return None
