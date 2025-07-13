import pandas as pd
import psycopg2
from datetime import datetime
import time
import sys

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': '19962011'
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


def load_csv_to_postgres(csv_path, table_name, schema='ds', truncate=False):
    """Загрузка данных из csv-файла в указанную таблицу PostgreSQL

    :param csv_path (str): Путь к csv-файлу.
    :param table_name (str): Название таблицы в БД
    :param schema (str): Схема БД (по умолчанию 'ds')
    :param truncate (bool): Нужно ли очищать таблицу перед загрузкой.
    """

    # Подключение к БД
    conn = None
    process_start_time = datetime.now() # Фиксируем реальное время начала процесса
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f'Ошибка подключения к Postgres: {e}')
        sys.exit(1)

    # Старт лога
    log_process(conn, f'load_{table_name}', 'IN_PROGRESS', start_time=process_start_time)

    print('Пауза 5 секунд')
    time.sleep(5)  # Пауза 5 секунд

    try:
        df = None
        # Проверка кодировки
        encoding_to_try = ['utf-8', 'windows-1251', 'cp1252', 'iso-8859-1']
        for enc in encoding_to_try:
            try:
                # Чтение csv-файла
                print(f'Чтение файла {csv_path}')
                df = pd.read_csv(csv_path, sep=';', encoding=enc)
                print(f'Прочитано {len(df)} строк.')
                break
            except UnicodeDecodeError:
                continue

        # Очистка таблицы (по необходимости)
        if truncate:
            with conn.cursor() as cursor:
                cursor.execute(f'TRUNCATE TABLE {schema}.{table_name}')
            print(f'Таблица {schema}.{table_name} очищена.')

        # Специальная обработка для ft_posting_f
        if table_name == 'ft_posting_f':
            df.columns = df.columns.str.lower()
            # Проверяем обязательные поля
            required_cols = ['oper_date', 'credit_account_rk', 'debet_account_rk']
            for col in required_cols:
                if col not in df.columns:
                    raise ValueError(f"Обязательный столбец {col} отсутствует в файле")

                # Заменяем пустые значения
                df[col] = df[col].fillna(pd.NaT if col == 'oper_date' else 0)

                # Проверяем, что нет NULL в обязательных полях
                if df[col].isnull().any():
                    raise ValueError(f"Обнаружены NULL значения в обязательном столбце {col}")

        # Определяем формат даты в зависимости от файла
        date_format = '%d.%m.%Y' if 'balance' in csv_path.lower() else '%Y-%m-%d'

        date_cols = [col for col in df.columns if 'date' in col.lower()]
        for col in date_cols:
            # Определяем формат в зависимости от таблицы и столбца
            if table_name == 'ft_posting_f' and col == 'oper_date':
                # Специальный формат для проводок
                formats_to_try = ['%d-%m-%Y', '%d.%m.%Y', '%Y-%m-%d']
            elif 'balance' in table_name.lower():
                # Формат для балансов
                formats_to_try = ['%d.%m.%Y', '%Y-%m-%d']
            else:
                # Стандартные форматы для остальных случаев
                formats_to_try = ['%Y-%m-%d', '%d.%m.%Y', '%d-%m-%Y']

            # Пробуем все форматы по очереди
            for fmt in formats_to_try:
                try:
                    df[col] = pd.to_datetime(df[col], format=fmt)
                    break
                except ValueError:
                    continue
            else:
                # Если ни один формат не подошел, пробуем автоопределение
                df[col] = pd.to_datetime(df[col], errors='coerce')

            # Проверяем результат
            if df[col].isnull().any():
                bad_rows = df[df[col].isnull()].index.tolist()
                print(f'Предупреждение: не удалось распознать даты в столбце {col} (строки: {bad_rows})')
                df = df.dropna(subset=[col])

        # Определение уникальных ключей для разных таблиц
        conflict_columns = {
            'md_exchange_rate_d': '(data_actual_date, currency_rk)',
            'md_ledger_account_s': '(ledger_account, start_date)',
            'ft_balance_f': '(on_date, account_rk)',
            'md_account_d': '(data_actual_date, account_rk)',
            'md_currency_d': '(currency_rk, data_actual_date)'
        }.get(table_name, None)

        # Загрузка данных в PostgreSQL
        print(f'Загрузка данных в {schema}.{table_name}.')
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                values = [
                    f"'{str(v)}'" if pd.notna(v) else 'NULL'
                    for v in row.values
                ]
                columns = ', '.join(row.index)
                values_str = ', '.join(values)

                # SQL-запрос
                query = f"""
                    INSERT INTO {schema}.{table_name} ({columns})
                    VALUES ({values_str})
                """

                if conflict_columns:
                    # Определяем поля для обновления
                    update_cols = [col for col in row.index
                                   if col not in conflict_columns.replace('(', '').replace(')', '').split(', ')]
                    update_set = ', '.join([f'{col} = EXCLUDED.{col}' for col in update_cols])
                    query += f"""
                        ON CONFLICT {conflict_columns}
                        DO UPDATE SET {update_set}
"""
                cursor.execute(query)

        # Успешное завершение
        process_end_time = datetime.now()  # Фиксируем реальное время окончания
        log_process(
            conn,
            f'load_{table_name}',
            'SUCCESS',
            start_time=process_start_time,
            end_time=process_end_time,
            rows_processed=len(df)
        )
        print(f'Успешно загружено {len(df)} строк.')

    except Exception as e:
        # Обработка ошибок
        process_end_time = datetime.now() # Фиксируем реальное время окончания
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
        conn.close()
        print('Подключение к БД закрыто.')


def check_balance(account_rk):
    """Проверяет текущий баланс указанного account_rk"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT on_date, account_rk, balance_out
                FROM ds.ft_balance_f
                WHERE account_rk = {account_rk}
                ORDER BY on_date DESC
                LIMIT 1
            """
            )
            balance = cursor.fetchone()
            if balance:
                print(f'Текущий баланс для account_rk = {account_rk}:')
                print(f'Дата: {balance[0]}, Сумма: {balance[2]}')
            else:
                print(f'Запись для account_rk = {account_rk} не найдена.')
    except Exception as e:
        print(f'Ошибка при проверке баланса: {e}')
    finally:
        if conn:
            conn.close()

print('Проверка текущего баланса')
check_balance(24656)

print('Запуск ETL-процесса')
load_csv_to_postgres('ft_balance_f.csv', 'ft_balance_f')
#
print('Проверка текущего баланса')
check_balance(24656)

#load_csv_to_postgres('ft_balance_f.csv', 'ft_balance_f')
#load_csv_to_postgres('ft_posting_f.csv', 'ft_posting_f', truncate=True)