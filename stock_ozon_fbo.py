import requests
import json
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from sqlalchemy import create_engine



def stock_ozon_fbo():

    # задаем даты выборки в формате озона
    data_since = datetime.now() - timedelta(days=0)
    data_since_format = data_since.strftime('%Y-%m-%d')
    data_time_since_format = data_since.strftime('%Y-%m-%d %H:%M:%S')
# получаем список остатков на FBO в Озон 

    payload = json.dumps({
    
    "limit": 1000,
    "offset": 0,
    "warehouse_type": "ALL"

    })
    headers = {
    'Client-Id': '871606',
    'Api-Key': 'b987a03c-2aa1-4afd-925e-22975db8e5bf',
    'Content-Type': 'application/json',
    }

    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"

    response = requests.request("POST", url, headers=headers, data=payload)
    data_all = json.loads(response.text)
    data = data_all['result']['rows']

    # ПРИСВАИВАЕМ В ДАТАФРЕЙМ КОЛОНКУ current_date (текущая дата) текущей датой
    df = pd.DataFrame(data)
    df['current_date'] = pd.to_datetime(data_since_format)
    df['itog'] = pd.to_datetime(data_time_since_format)


    # Уданные подключенияк БД PostgreSQL
    db_username = 'postgres'
    db_password = '789'
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'sale_mp'
    table_name = 'stock_fbo_ozon'

    # Создание строки подключения к базе данных
    connection = psycopg2.connect(
    user=db_username,
    password=db_password,
    host=db_host,
    port=db_port,
    database=db_name
    )       

    # получае из готовой БД последнюю дату обновления
    cursor = connection.cursor()

    # Выполняем запрос для получения последней даты из колонки current_date
    query = f'SELECT MAX("current_date") AS last FROM public.stock_fbo_ozon LIMIT 10000'
    cursor.execute(query)

    # Получаем результат запроса, и преобразуем в простой формат даты
    last_date_result = cursor.fetchone()[0]
    last_date = last_date_result.strftime('%Y-%m-%d')
    print('последняя дата обновления ', last_date)

    # Закрываем курсор и соединение
    cursor.close()
    connection.close()

    # если дата последней обновления равна дате с которой мы начинаем выборку, то новых данных нет
    if last_date == data_since_format:
        data_since_format,
        print('новых нет')
    # инако обновляем таблицу и добавляем новые данные
    else:
        engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print('обновили', last_date, data_since_format)

  

    return print('остатки заебись', data_since_format)


# orders = stock_ozon_fbo()
