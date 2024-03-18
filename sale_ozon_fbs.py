import requests
import json
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# задаем даты выборки в формате озона
# today = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" 
# data_since = datetime.now() - timedelta(days=30)
# data_since_format = data_since.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" 

# today = '2023-07-02T15:36:57Z'
# data_since_format = '2023-01-01T15:36:57Z' 

def sale_ozon_fbs():


    # задаем даты выборки в формате озона
    today = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" 
    data_since = datetime.now() - timedelta(days=30)
    data_since_format = data_since.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" 

# получаем список заказов 

    payload = json.dumps({
    "dir": "ASC",
    "filter": {
        "since": data_since_format,
        "status": "", # ожидают отгрузки? cancelled awaiting_deliver delivered 
        "to": today
    },
    "limit": 1000,
    "offset": 0,
    "translit": True,
    "with": {
        "analytics_data": True,
        "financial_data": True
    }
    })
    headers = {
    'Client-Id': '871606',
    'Api-Key': 'b987a03c-2aa1-4afd-925e-22975db8e5bf',
    'Content-Type': 'application/json',
    }

    url = "https://api-seller.ozon.ru/v3/posting/fbs/list"

    response = requests.request("POST", url, headers=headers, data=payload)

    data_all = json.loads(response.text)
    
    data = data_all["result"]["postings"]
        
    conn = psycopg2.connect(dbname='sale_mp', user='postgres', password='789', host='localhost', port=5432)
    cur = conn.cursor()

    # Формирование SQL запроса для вставки или обновления данных
    for item in data:
        products_json = json.dumps(item["products"])  # Преобразование продуктов в строку JSON
        
        cur.execute(
            "INSERT INTO sale_ozon_fbs (order_id, order_number, posting_number, status, in_process_at, products, analytics_data, financial_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (posting_number) DO UPDATE SET "
            "order_id = EXCLUDED.order_id, "
            "order_number = EXCLUDED.order_number, "
            "status = EXCLUDED.status, "
            "in_process_at = EXCLUDED.in_process_at, "
            "products = EXCLUDED.products, "
            "analytics_data = EXCLUDED.analytics_data, "
            "financial_data = EXCLUDED.financial_data",
            (item["order_id"], item["order_number"], item["posting_number"], item["status"], item["in_process_at"], products_json, json.dumps(item["analytics_data"]), 
            json.dumps(item["financial_data"])
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    # file_path = "tapp/ozon_fbo/data6.json"
    

    # # Открываем файл на запись
    # with open(file_path, "w", encoding='utf-8') as json_file:
    #     # Записываем данные в файл в формате JSON
    #     json.dump(data, json_file, indent=4, )

    return print('ФБС_заебись', today)


# orders = sale_ozon_fbs()
