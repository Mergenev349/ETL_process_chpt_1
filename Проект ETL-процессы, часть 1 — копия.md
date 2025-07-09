Скипты по созданию таблиц для БД.

CREATE SCHEMA DS;

CREATE TABLE ds.ft_balance_f(
on_date DATE NOT NULL,
account_rk INT NOT NULL,
currency_rk INT,
balance_out DECIMAL(15, 2),
PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE ds.ft_posting_f(
oper_date DATE NOT NULL,
credit_account_rk INT NOT NULL,
debet_account_rk INT NOT NULL,
credit_amount DECIMAL(15, 2),
debet_amount DECIMAL(15, 2)
);

CREATE TABLE ds.md_account_d(
data_actual_date DATE NOT NULL,
data_actual_end_date DATE NOT NULL,
account_rk INT NOT NULL,
account_number TEXT NOT NULL,
char_type CHAR(1) NOT NULL,
currency_rk INT NOT NULL,
currency_code VARCHAR(3) NOT NULL,
PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE ds.md_currency_d(
currency_rk INT NOT NULL,
data_actual_date DATE NOT NULL,
data_actual_end_date DATE,
currency_code VARCHAR(3),
code_iso_char VARCHAR(3),
PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE ds.md_exchange_rate_d(
data_actual_date DATE NOT NULL,
data_actual_end_date DATE,
currency_rk INT NOT NULL,
reduced_cource DECIMAL(22, 6),
code_iso_num VARCHAR(3),
PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE ds.md_ledger_account_s(
chapter CHAR(1),
chapter_name TEXT,
section_number INT,
section_name TEXT,
subsection_name TEXT,
ledger1_account INT,
ledger1_account_name TEXT,
ledger_account INT NOT NULL,
ledger_account_name TEXT,
characteristic CHAR(1),
is_resident INT,
is_reserve INT,
is_reserved INT,
is_loan INT,
is_reserved_assets INT,
is_overdue INT,
is_interest INT,
pair_account TEXT,
start_date DATE NOT NULL,
end_date DATE,
is_rub_only INT,
min_term VARCHAR(1),
min_term_measure VARCHAR(1),
max_term VARCHAR(1),
max_term_measure VARCHAR(1),
ledger_acc_full_name_translit VARCHAR(1),
is_revaluation VARCHAR(1),
is_correct VARCHAR(1),
PRIMARY KEY (ledger_account, start_date)
);


CREATE SCHEMA LOGS;

CREATE TABLE logs.etl_logs(
log_id SERIAL PRIMARY KEY NOT NULL,
process_name TEXT,
start_time TIMESTAMP NOT NULL,
end_time TIMESTAMP,
rows_processed INT DEFAULT 0,
process_status VARCHAR(20) NOT NULL 
    CHECK(process_status IN ('SUCCESS', 'FAILED', 'IN_PROGRESS')),
error_message TEXT,
duration INTERVAL GENERATED ALWAYS AS (end_time - start_time) STORED
CONSTRAINT valid_end_time CHECK(
    (process_status IN ('SUCCESS', 'FAILED') AND end_time IS NOT NULL) OR
    (process_status = 'IN_PROGRESS' AND end_time IS NULL)
)
);

Ссылка на видео на яндекс диске: https://disk.yandex.ru/i/iNl8PRA9b1-0Eg
