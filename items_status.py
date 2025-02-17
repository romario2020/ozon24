import requests
import json
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# получаем список активных и архивныйх артикулов, сто статусом активности


def items_status():
    # получаем всех активных товаров
    url = "https://api-seller.ozon.ru/v3/product/list"

    payload = json.dumps(
        {"filter": {"visibility": "ALL"}, "last_id": "", "limit": 1000}
    )
    headers = {
        "Client-Id": "871606",
        "Api-Key": "b987a03c-2aa1-4afd-925e-22975db8e5bf",
        "Content-Type": "application/json",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data_activ = json.loads(response.text)
    data_items_activ = data_activ["result"]["items"]
    df_active = pd.DataFrame(data_items_activ)

    # получаем всех АРХИВНЫХ товаров
    url = "https://api-seller.ozon.ru/v3/product/list"

    payload = json.dumps(
        {"filter": {"visibility": "ARCHIVED"}, "last_id": "", "limit": 1000}
    )
    headers = {
        "Client-Id": "871606",
        "Api-Key": "b987a03c-2aa1-4afd-925e-22975db8e5bf",
        "Content-Type": "application/json",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    data_arhive = json.loads(response.text)
    data_items_arhiv = data_arhive["result"]["items"]
    df_arhive = pd.DataFrame(data_items_arhiv)

    # объеденяем активные и архивные в единую таблицу
    df_items = pd.concat([df_active, df_arhive])

    # Сортировка датафрейма по столбцу 'offer_id' в порядке возрастания
    df_items.sort_values(by="offer_id", inplace=True)
    print(df_items.head(5))

    cols = df_items.columns.tolist()
    cols.insert(0, cols.pop(cols.index("offer_id")))
    df_items = df_items.reindex(columns=cols)

    # подключенияк БД PostgreSQL
    db_username = "postgres"
    db_password = "kjaWWmk!H12iznkwqzfhwugi"
    db_host = "localhost"
    db_port = "5432"
    db_name = "sale_mp"
    table_name = "items_ozon"

    engine = create_engine(
        f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    # записываем таблицу в БД
    df_items.to_sql(table_name, engine, if_exists="replace", index=False)

    return print("остатки заебись", df_items.tail(10))


# items_status()
