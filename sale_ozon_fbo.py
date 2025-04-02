import requests
import json
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# задаем даты выборки в формате озона
# data_since_format = '2023-12-01T15:36:57Z'
# today = '2024-03-31T15:36:57Z'

# 2024-02-27T13:40:13.337Z 2024-02-26T13:40:13.337Z

# today = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
# data_since = datetime.now() - timedelta(days=65)
# data_since_format = data_since.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def sale_ozon_fbo():
    today = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    data_since = datetime.now() - timedelta(days=10)
    data_since_format = data_since.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"

    payload = json.dumps(
        {
            "dir": "ASC",
            "filter": {"since": data_since_format, "status": "", "to": today},
            "limit": 1000,
            "offset": 0,
            "translit": True,
            "with": {"analytics_data": True, "financial_data": True},
        }
    )
    headers = {
        "Client-Id": "871606",
        "Api-Key": "b987a03c-2aa1-4afd-925e-22975db8e5bf",
        "Content-Type": "application/json",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data_all = json.loads(response.text)
    data = data_all["result"]

    # df = pd.DataFrame(postings)

    # Данные подключения к базе данных
    # db_params = {
    # 'dbname': 'sale_mp',
    # 'user': 'postgres',
    # 'password': '789',
    # 'host': 'localhost',
    # 'port': '5432'
    # }

    # Создание подключения к базе данных

    conn = psycopg2.connect(
        "dbname=sale_mp user=postgres password=kjaWWmk!H12iznkwqzfhwugi host=localhost port=5432"
    )
    cur = conn.cursor()

    # # Создание таблицы
    #     cur.execute('''
    #         CREATE TABLE sale_ozon_fbo (
    #             order_id BIGINT,
    #             order_number VARCHAR(255),
    #             posting_number VARCHAR(255),
    #             status VARCHAR(50),
    #             cancel_reason_id INTEGER,
    #             created_at TIMESTAMP,
    #             in_process_at TIMESTAMP,
    #             products JSONB,
    #             analytics_data JSONB,
    #             financial_data JSONB,
    #             additional_data JSONB
    #         )
    #     ''')

    for item in data:
        products_json = json.dumps(item["products"])  # Convert products to JSON string

        cur.execute(
            "INSERT INTO sale_ozon_fbo (order_id, order_number, posting_number, status, cancel_reason_id, created_at, in_process_at, products, analytics_data, financial_data, additional_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (posting_number) DO UPDATE SET "
            "order_number = EXCLUDED.order_number, posting_number = EXCLUDED.posting_number, "
            "status = EXCLUDED.status, cancel_reason_id = EXCLUDED.cancel_reason_id, "
            "created_at = EXCLUDED.created_at, in_process_at = EXCLUDED.in_process_at, "
            "products = EXCLUDED.products, "
            "analytics_data = EXCLUDED.analytics_data, "
            "financial_data = EXCLUDED.financial_data, "
            "additional_data = EXCLUDED.additional_data ",
            (
                item["order_id"],
                item["order_number"],
                item["posting_number"],
                item["status"],
                item["cancel_reason_id"],
                item["created_at"],
                item["in_process_at"],
                json.dumps(item["products"]),
                json.dumps(item["analytics_data"]),
                json.dumps(item["financial_data"]),
                json.dumps(item["additional_data"]),
            ),
        )

    # Закрытие курсора и подтверждение транзакции financial_data
    conn.commit()
    cur.close()
    conn.close()

    return print("заебись ФБО", today)


# sale_ozon_fbo()