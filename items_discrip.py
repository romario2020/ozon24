import requests
import json
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy


# получаем список активных и архивныйх артикулов

def items_discrip():
    # получаем всех активных товаров
    url = "https://api-seller.ozon.ru/v3/products/info/attributes"

    payload = json.dumps(
         {
                "filter": {
                "visibility": "ALL"
                },
                "last_id": "",
                "limit": 1000
        }
    )
    headers = {
    'Client-Id': '871606',
    'Api-Key': 'b987a03c-2aa1-4afd-925e-22975db8e5bf',
    'Content-Type': 'application/json',
    'Cookie': 'abt_data=60477e1e3d51763bd6ff189aaa324c47:81927a4f5010009a8e75de2d215d772f414c24782ec5646c27f8018ebb465226ef8408bb44a95e04c248de39c9cf126f0e6fd0c6229b3121849bb5a5b6a24426aceb13a90c09ecb86c30bfe3b4fa5bfea1e804b46f148f368b8570cc79fa84d1ab427d13a47348fcc45e3e63cdccae62624804e81dc47306049e93e83bd7f71a2aed3c1243e8567018f476c6f71ef58a54dca7a1f8a67647435810de58798d6fd79eab7e2f4b59e47eda7314573ddc18704880cdb9c99d4fe9afc20b1d805f78ad7061abf6fdd8117de306cf52e7ef80310bbb00bbad70c2d8d364e80294e35c62aa4b71d5022d3a60960a28476d298b97a4c22c4ada6b60c2421851ec9a10dc45bbdc05a9d8a5e4cef015f57e75dc927ddc09056a938207e040beddba260eee62aefb36aa589c5f2f9db39fce13c8c5b357a72361f14c34affeb79d3fcae9353a4c5bee363d43ebba86a0632a9cea05ab5f565d9e1f9ae992cfbb269df8b591c6046a83b32552d5e0da2b49007310c95824bd0d719b944d638cec99bff617319ee4e4460d3605baf6e145af681425c6; __cf_bm=4hzjMsfksiT0RuZJd05QG6gKfbExsD18blZsN_ECbDo-1708351774-1.0-AdI2NjqJh1M2iafaDNtS1BDfPZpm8+ZowSpVwSFu4IHgY3uhhFjew3PpeZbAS7AfyB5QQR9SPdVuMTTrpoPWjAo='
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data_activ = json.loads(response.text)
    data_items_activ = data_activ["result"]
    df_active = pd.DataFrame(data_items_activ)

     # получаем всех АРХИВНЫХ товаров
    url = "https://api-seller.ozon.ru/v3/products/info/attributes"

    payload = json.dumps(
         {
                "filter": {
                "visibility": "ARCHIVED"
                },
                "last_id": "",
                "limit": 1000
        }
    )
    headers = {
    'Client-Id': '871606',
    'Api-Key': 'b987a03c-2aa1-4afd-925e-22975db8e5bf',
    'Content-Type': 'application/json',
    'Cookie': 'abt_data=60477e1e3d51763bd6ff189aaa324c47:81927a4f5010009a8e75de2d215d772f414c24782ec5646c27f8018ebb465226ef8408bb44a95e04c248de39c9cf126f0e6fd0c6229b3121849bb5a5b6a24426aceb13a90c09ecb86c30bfe3b4fa5bfea1e804b46f148f368b8570cc79fa84d1ab427d13a47348fcc45e3e63cdccae62624804e81dc47306049e93e83bd7f71a2aed3c1243e8567018f476c6f71ef58a54dca7a1f8a67647435810de58798d6fd79eab7e2f4b59e47eda7314573ddc18704880cdb9c99d4fe9afc20b1d805f78ad7061abf6fdd8117de306cf52e7ef80310bbb00bbad70c2d8d364e80294e35c62aa4b71d5022d3a60960a28476d298b97a4c22c4ada6b60c2421851ec9a10dc45bbdc05a9d8a5e4cef015f57e75dc927ddc09056a938207e040beddba260eee62aefb36aa589c5f2f9db39fce13c8c5b357a72361f14c34affeb79d3fcae9353a4c5bee363d43ebba86a0632a9cea05ab5f565d9e1f9ae992cfbb269df8b591c6046a83b32552d5e0da2b49007310c95824bd0d719b944d638cec99bff617319ee4e4460d3605baf6e145af681425c6; __cf_bm=4hzjMsfksiT0RuZJd05QG6gKfbExsD18blZsN_ECbDo-1708351774-1.0-AdI2NjqJh1M2iafaDNtS1BDfPZpm8+ZowSpVwSFu4IHgY3uhhFjew3PpeZbAS7AfyB5QQR9SPdVuMTTrpoPWjAo='
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    data_arhive = json.loads(response.text)
    data_items_arhiv = data_arhive["result"]
    df_arhive = pd.DataFrame(data_items_arhiv)


#объеденяем активные и архивные в единую таблицу
    df_items = pd.concat([df_active, df_arhive]) 
    
# Сортировка датафрейма по столбцу 'offer_id' в порядке возрастания
    df_items.sort_values(by='offer_id', inplace=True)
    print(df_items.head(5))

    cols = df_items.columns.tolist()
    cols.insert(0, cols.pop(cols.index('offer_id')))
    df_items = df_items.reindex(columns=cols)

    # удаляем лишние столбцы
    del df_items["images"], df_items["images360"], df_items["pdf_list"], df_items["complex_attributes"]


    # подключенияк БД PostgreSQL
    db_username = 'postgres'
    db_password = 'postgres'
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'sale_mp'
    table_name = 'items_description'

    
    engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

    #записываем таблицу в БД
    df_items.to_sql(table_name, engine, if_exists='replace', index=False, dtype={'attributes': sqlalchemy.types.JSON,'complex_attributes': sqlalchemy.types.JSON})

  

    return df_items, print('описание заебись')




# items_discrip()