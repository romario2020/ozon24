import requests
import json
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import psycopg2
from sqlalchemy import create_engine 


# задаем даты выборки в формате озона

# today = '2023-11-01T15:36:57Z'
# data_since_format = '2023-09-01T15:36:57Z' 
# 2024-02-27T13:40:13.337Z 2024-02-26T13:40:13.337Z

today = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" 
data_since = datetime.now() - timedelta(days=1)
data_since_format = data_since.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" 


def list_orders():
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"

    payload = json.dumps({
    "dir": "ASC",
    "filter": {
        "since": data_since_format,
        "status": "",
        "to": today
    },
    "limit": 500,
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
    'Cookie': 'abt_data=60477e1e3d51763bd6ff189aaa324c47:81927a4f5010009a8e75de2d215d772f414c24782ec5646c27f8018ebb465226ef8408bb44a95e04c248de39c9cf126f0e6fd0c6229b3121849bb5a5b6a24426aceb13a90c09ecb86c30bfe3b4fa5bfea1e804b46f148f368b8570cc79fa84d1ab427d13a47348fcc45e3e63cdccae62624804e81dc47306049e93e83bd7f71a2aed3c1243e8567018f476c6f71ef58a54dca7a1f8a67647435810de58798d6fd79eab7e2f4b59e47eda7314573ddc18704880cdb9c99d4fe9afc20b1d805f78ad7061abf6fdd8117de306cf52e7ef80310bbb00bbad70c2d8d364e80294e35c62aa4b71d5022d3a60960a28476d298b97a4c22c4ada6b60c2421851ec9a10dc45bbdc05a9d8a5e4cef015f57e75dc927ddc09056a938207e040beddba260eee62aefb36aa589c5f2f9db39fce13c8c5b357a72361f14c34affeb79d3fcae9353a4c5bee363d43ebba86a0632a9cea05ab5f565d9e1f9ae992cfbb269df8b591c6046a83b32552d5e0da2b49007310c95824bd0d719b944d638cec99bff617319ee4e4460d3605baf6e145af681425c6; __cf_bm=4hzjMsfksiT0RuZJd05QG6gKfbExsD18blZsN_ECbDo-1708351774-1.0-AdI2NjqJh1M2iafaDNtS1BDfPZpm8+ZowSpVwSFu4IHgY3uhhFjew3PpeZbAS7AfyB5QQR9SPdVuMTTrpoPWjAo='
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data_all = json.loads(response.text)
    data = data_all["result"]

    file_path = "C:/Users/roman/Downloads/data3.json"
    
    # Открываем файл на запись
    with open(file_path, "w", encoding='utf-8') as json_file:
        # Записываем данные в файл в формате JSON
        json.dump(data, json_file, indent=4, )
    
    
    return print(data)

list_orders()