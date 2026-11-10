import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlglot import parse_one, exp
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import ClickHouseError
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загрузка кредов из .env
load_dotenv()


def get_clickhouse_client():
    """Создает и возвращает клиент ClickHouse, используя переменные окружения."""
    return get_client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', 8123)),
        username=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', '')
    )


def ensure_columns_exist(client, schema: str, target: str):
    """
    Утилита для ручного добавления системных колонок (actual_begin, actual_end).
    Вызывать отдельно, если load_data_to_clickhouse выдала ошибку об их отсутствии.
    """
    existing_cols = client.query(
        "SELECT name FROM system.columns WHERE database = {db: String} AND table = {table: String}",
        parameters={'db': schema, 'table': target}
    ).result_rows
    existing_cols = [col[0] for col in existing_cols]

    if 'actual_begin' not in existing_cols:
        logger.info(f"Колонка actual_begin отсутствует в {schema}.{target}. Добавляем...")
        client.command(f"ALTER TABLE {schema}.{target} ADD COLUMN actual_begin DateTime DEFAULT now('Europe/Moscow')")

    if 'actual_end' not in existing_cols:
        logger.info(f"Колонка actual_end отсутствует в {schema}.{target}. Добавляем...")
        client.command(f"ALTER TABLE {schema}.{target} ADD COLUMN actual_end Nullable(DateTime) DEFAULT NULL")

    logger.info("Проверка и добавление колонок завершены.")


def load_data_to_clickhouse(
        client,
        source: str | Path,
        schema: str,
        target: str,
        update_type: str,
        read_kwargs: Optional[dict] = None
) -> Optional[tuple[datetime, int]]:
    """
    Загружает данные из XLSX/CSV в ClickHouse.
    """
    if read_kwargs is None:
        read_kwargs = {}

    try:
        source = Path(source)
        file_extension = source.suffix.lower()

        logger.info(f"Чтение файла: {source}")
        if file_extension == '.xlsx':
            df = pd.read_excel(source)
        elif file_extension == '.csv':
            df = pd.read_csv(source, **read_kwargs)
        else:
            raise ValueError(f"Неподдерживаемый формат: {file_extension}")

        # 1. ВСЕГДА добавляем служебные колонки в DataFrame ДО проверки таблицы
        df['actual_begin'] = pd.Timestamp.now(tz='Europe/Moscow')
        df['actual_end'] = None

        # Проверка существования таблицы
        table_exists = client.query(
            "SELECT count(*) > 0 FROM system.tables WHERE database = {db: String} AND name = {table: String}",
            parameters={'db': schema, 'table': target}
        ).result_rows[0][0]

        if not table_exists:
            # Таблицы нет - создаем её сразу с нужными колонками
            logger.info(f"Таблица {schema}.{target} не найдена. Создаем...")
            schema_ddl = pd.io.sql.get_schema(df.reset_index(drop=True), name='"')
            # Заставляем ClickHouse сделать actual_end типа Nullable(DateTime), а не String
            schema_ddl = schema_ddl.replace('"actual_end" TEXT', '"actual_end" Nullable(DateTime)')

            create_exp = exp.Create(**{
                **parse_one(schema_ddl).args,
                'properties': exp.Properties(
                    expressions=[exp.EngineProperty(this='MergeTree() ORDER BY ()')]
                )
            })
            ddl_query = create_exp.sql('clickhouse').replace('""""', f'{schema}.{target}')
            client.command(ddl_query)
        else:
            # 2. Таблица есть. Проверяем, есть ли в ней служебные колонки
            existing_cols = client.query(
                "SELECT name FROM system.columns WHERE database = {db: String} AND table = {table: String}",
                parameters={'db': schema, 'table': target}
            ).result_rows
            existing_cols = [col[0] for col in existing_cols]

            missing_cols = [col for col in ['actual_begin', 'actual_end'] if col not in existing_cols]

            if missing_cols:
                # 3. Если колонок нет - выводим уведомление и ПРЕКРАЩАЕМ работу
                logger.error(f"ОШИБКА: Таблица {schema}.{target} уже существует, но в ней нет колонок: {missing_cols}.")
                logger.error(f"Выполнение остановлено. Ничего не изменено.")
                logger.error(
                    f"Если вы хотите добавить колонки, запустите функцию вручную: ensure_columns_exist(client, '{schema}', '{target}')")
                return None

        # Логика обновления
        if update_type == 'full':
            temp_table = f"{schema}.temp_{target}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            logger.info(f"Создаем бэкап во временной таблице {temp_table}")
            client.command(
                f"CREATE TABLE {temp_table} ENGINE = MergeTree() ORDER BY tuple() AS SELECT * FROM {schema}.{target}")

            try:
                logger.info("Помечаем старые данные как неактуальные...")
                client.command(
                    f"ALTER TABLE {schema}.{target} UPDATE actual_end = now('Europe/Moscow') WHERE actual_end IS NULL")

                logger.info("Вставляем новые данные...")
                client.insert_df(f"{schema}.{target}", df)
                logger.info("Успешно загружено (full update).")

            except Exception as e:
                logger.error(f"Ошибка: {e}. Восстанавливаем из бэкапа...")
                client.command(f"TRUNCATE TABLE {schema}.{target}")
                client.command(f"INSERT INTO {schema}.{target} SELECT * FROM {temp_table}")
                return None
            finally:
                client.command(f"DROP TABLE IF EXISTS {temp_table}")

        elif update_type == 'incremental':
            logger.info("Вставляем новые данные (incremental)...")
            client.insert_df(f"{schema}.{target}", df)
            logger.info("Успешно загружено (incremental update).")

        else:
            raise NotImplementedError(f"Тип обновления {update_type} не поддерживается")

        return datetime.now(), df.shape[0]

    except ClickHouseError as e:
        logger.error(f"Ошибка ClickHouse: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return None


if __name__ == "__main__":

    # Пример использования
    client = get_clickhouse_client()
    load_data_to_clickhouse(client, "data.xlsx", "default", "test_table", "full")

    # Пример: как можно вручную добавить колонки, если скрипт выдал ошибку
    # client = get_clickhouse_client()
    # ensure_columns_exist(client, "default", "test_table")

    pass