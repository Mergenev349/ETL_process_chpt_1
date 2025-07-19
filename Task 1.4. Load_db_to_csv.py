import psycopg2
import csv
from datetime import datetime
import sys

db_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "dwh",
    "user": "postgres",
    "password": "19962011"
}


def log_process (conn, process_name, status, rows_processed=0, start_time=None, end_time=None, error_msg=None):
    """
    :param conn(psycopg2.connection): Подключение к PostgresSQL,
    :param process_name(str): Название процесса,
    :param status(str): Статус процесса,
    :param rows_processed(int): Количество обработанных строк. По умолчанию 0
    :param start_time: datetime - реальное время начала процесса
    :param end_time: datetime - реальное время окончания
    :param error_msg(str): Текст ошибки. По умолчанию None.
    """

    # Определяем время начала и окончания записи
    if start_time is None:
        start_time = datetime.now()
    if status in ('SUCCESS', 'FAILED') and end_time is None:
        end_time = datetime.now()

    query = """
        INSERT INTO logs.etl_logs
            (process_name, start_time, end_time, process_status, rows_processed, error_message)
        VALUES
        (%s, %s, %s, %s, %s, %s)
    """

    with conn.cursor() as cursor: # Выполняем запрос на вставку данных
        cursor.execute(query, (
            process_name,
            start_time,
            end_time,
            status,
            rows_processed,
            error_msg
        ))
    conn.commit() #Фиксируем изменения в БД



def load_database_to_csv(csv_file, table_name= 'dm_f101_round_f', schema= 'dm'):
    """
    Загружает данные в csv-файл из таблицы в БД.

    :param csv_file(str): Файл формата .csv, в который переносим данные из БД.
    :param table_name(str): Таблица в БД, из которой экспортируем данные.
    :param schema(str): Схема в БД, в которой находится таблица (по умолчанию dm)
    """

    # Подключение к БД
    conn = None
    process_start_time = datetime.now()  # Фиксируем реальное время начала процесса
    try:
        conn = psycopg2.connect(**db_config)
        query = f'SELECT * FROM {schema}.{table_name}'
    except Exception as e:
        print(f'Ошибка подключения к Postgres: {e}')
        sys.exit(1)


    try:
        with conn.cursor() as cursor:
            cursor.execute(query)

            #Подсчёт строк таблицы
            rows = cursor.fetchall()
            rows_count = len(rows)

            #Возвращаем курсор в начало
            cursor.execute(query)

            with open(csv_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([desc[0] for desc in cursor.description])  # Записываем заголовки
                writer.writerows(cursor)  # Записываем данные

        # Успешное завершение
        process_end_time = datetime.now()  # Фиксируем реальное время окончания
        log_process(
            conn,
            f'load_{table_name}',
            'SUCCESS',
            start_time=process_start_time,
            end_time=process_end_time,
            rows_processed=rows_count
        )
        print(f'Успешно загружено {rows_count} строк в {csv_file}.')

    except Exception as e:
        # Обработка ошибок
        process_end_time = datetime.now()  # Фиксируем реальное время окончания
        log_process(
            conn,
            f'load_{table_name}',
            'FAILED',
            start_time=process_start_time,
            end_time=process_end_time,
            error_msg=str(e)
        )
        print(f'Ошибка: {e}')

    finally:
        # Закрытие подключения
        if conn:
            conn.close()
            print('Подключение к БД закрыто.')


def load_csv_to_database(csv_file, table_name='dm_f101_round_f_v2', schema='dm', truncate=False):
    """
    Загружает данные из CSV-файла в таблицу PostgreSQL

    :param csv_file(str): Путь к CSV-файлу
    :param table_name(str): Имя целевой таблицы
    :param schema(str): Схема БД (по умолчанию 'dm')
    :param truncate(bool): Очищать таблицу перед загрузкой (по умолчанию False)
        """
    conn = None
    process_start_time = datetime.now()

    try:
        conn = psycopg2.connect(**db_config)

        # Пробуем определить кодировку файла
        try:
            import chardet
            with open(csv_file, 'rb') as f:
                rawdata = f.read(10000) # Читаем первые 10Кб для определения
                encoding = chardet.detect(rawdata)['encoding'] or 'utf-8'
        except ImportError:
            encoding = 'utf-8'


        with conn.cursor() as cursor:
            # Очистка таблицы по необходимости
            if truncate:
                cursor.execute(f'TRUNCATE TABLE {schema}.{table_name}')
                conn.commit()
                print(f'Таблица {schema}.{table_name} полностью очищена.')

            try:
                with open(csv_file, 'r', encoding=encoding) as f:
                    rows_count = sum(1 for _ in f) - 1 # Вычитаем заголовки
            except UnicodeDecodeError:
                # Пробуем альтернативные кодировки
                for alt_enc in ['windows-1251', 'cp1251', 'iso-8859-1']:
                    try:
                        with open(csv_file, 'r', encoding=alt_enc) as f:
                            rows_count = sum(1 for _ in f) - 1
                            encoding = alt_enc
                            break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError('Не удалось определить кодировку файла')

            with open(csv_file, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                headers = next(reader)
                placeholders = ', '.join(['%s'] * len(headers))
                insert_query = f"INSERT INTO {schema}.{table_name} VALUES ({placeholders})"

                for row in reader:
                    cursor.execute(insert_query, row)
                # next(f) # Пропускаем заголовок
                # cursor.copy_expert(
                #     f'COPY {schema}.{table_name} FROM STDIN WITH CSV',
                #     f
                # )

            conn.commit()

        # Логирование успеха
        log_process(
            conn,
            f'import_{table_name}',
            'SUCCESS',
            rows_processed=rows_count,
            start_time=process_start_time,
            end_time=datetime.now()
        )
        print(f'Успешно загружено {rows_count} строк в {schema}.{table_name}')

    except Exception as e:
        # Логирование ошибки
        if conn:
            log_process(
                conn,
                f'import_{table_name}',
                'FAILED',
                start_time=process_start_time,
                end_time=datetime.now(),
                error_msg=str(e)
            )
        print(f'Ошибка при загрузке данных: {e}')

    finally:
        if conn:
            conn.close()
            print('Подключение к БД закрыто.')



load_csv_to_database('deal_info.csv', table_name='deal_info_backup', schema='rd')
load_csv_to_database('product_info.csv', table_name='product_backup', schema='rd')

#Ссылка на видео: https://disk.yandex.ru/i/sjYOKV-JQKSk4g