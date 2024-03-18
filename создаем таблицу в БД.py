import psycopg2

# Параметры подключения к базе данных
dbname = "sale_mp"
user = "postgres"
password = "789"
host = "localhost"
port = 5432

# SQL запрос для создания таблицы sale_ozon_fbs
create_table_query = """
CREATE TABLE sale_ozon_fbs (
    posting_number VARCHAR(50) UNIQUE,
    order_id BIGINT,
    order_number VARCHAR(20),
    status VARCHAR(20),
    delivery_method JSONB,
    tracking_number VARCHAR(50),
    tpl_integration_type VARCHAR(20),
    in_process_at TIMESTAMP,
    shipment_date TIMESTAMP,
    delivering_date TIMESTAMP,
    cancellation JSONB,
    customer JSONB,
    products JSONB,
    addressee JSONB,
    barcodes JSONB,
    analytics_data JSONB,
    financial_data JSONB,
    is_express BOOLEAN,
    requirements JSONB,
    parent_posting_number VARCHAR(50),
    available_actions JSONB,
    multi_box_qty INTEGER,
    is_multibox BOOLEAN,
    substatus VARCHAR(50),
    prr_option VARCHAR(50)
);
"""

# Подключение к базе данных и создание таблицы
try:
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    print("Таблица 'sale_ozon_fbs' успешно создана в базе данных 'sale_mp'")
except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL:", error)
finally:
    if conn:
        conn.close()


# file_path = "C:/Users/roman/Downloads/data3.json"

# # Открываем файл на запись
    # with open(file_path, "w", encoding='utf-8') as json_file:
    #     # Записываем данные в файл в формате JSON
    #     json.dump(data, json_file, indent=4, )