import psycopg2

# Параметры подключения к базе данных локалка
# dbname = "sale_mp"
# user = "postgres"
# password = "789"
# host = "localhost"
# port = 5432

# сервер
# Параметры подключения к базе данных
dbname = "sale_mp"
user = "postgres"
password = "kjaWWmk!H12iznkwqzfhwugi"
host = "localhost"
port = 5432 



# SQL запрос для создания таблицы sale_ozon_fbs
create_table_query = """
CREATE TABLE catalog_items (
    offer_id VARCHAR(50) UNIQUE,
    order_number VARCHAR(20),
    name VARCHAR(50),
    active VARCHAR(10),
    Группа VARCHAR(25),
    Подгруппа VARCHAR(25),
    цвет_осн VARCHAR(25),
    цвет2 VARCHAR(25),
    тип VARCHAR(25),
    поставщик VARCHAR(25),
    назначение VARCHAR(25)

);
"""

# Подключение к базе данных и создание таблицы
try:
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    print("catalog_items' успешно создана в базе данных 'sale_mp'")
except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL:", error)
finally:
    if conn:
        conn.close()
