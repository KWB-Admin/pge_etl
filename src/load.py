import polars, logging
import psycopg2 as pg
from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import Dict
from contextlib import contextmanager
from .models import Credentials, SourceConfig, ETLConfig, ETLMetrics
from .exceptions import LoadError

logger = logging.getLogger("pge_etl.load")


@contextmanager
def get_pg_connection(db_name: str, creds: Credentials):
    """
    Context manager for pg connection.

    Args:
        db_name: str, name of database to connect to.
        creds: Dict, dictionary of credentials used for ETL
    Yields:
        con: pg.extensions.connection, psycopg connection to pg database
    """
    con = None
    try:
        con = pg.connect(
            "dbname=%s user=%s host=%s password=%s"
            % (db_name, creds.user, creds.host, creds.password)
        )
        con.autocommit = True
        logger.debug("Successfully connected to %s database" % (db_name))
        yield con
    except pg.OperationalError as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if con:
            con.close()


def validate_table_exists(
    con: pg.extensions.connection, schema_name: str, table_name: str
) -> bool:
    """
    This tests a to ensure the table we'll be writing to exists in
    the postgres schema provided.

    Args:
        con: pg.extensions.connection, psycopg connection to pg
            database
        schema_name: str, name of postgres schema
        table_name: str, name of table
    Returns:
        True if table exists, False otherwise
    """
    try:
        with con.cursor() as cur:
            command = sql.SQL(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )  
                """
            )
            cur.execute(command, (schema_name, table_name))
            exists = cur.fetchone()[0]
        if exists:
            logger.debug(f"Table {schema_name}.{table_name} exists")
        else:
            logger.error(f"Table {schema_name}.{table_name} does not exist")

        return exists
    except Exception as e:
        logger.error(f"Error checking table existance: {e}")
        return False


def bulk_load_data(
    data: polars.DataFrame,
    etl_config: ETLConfig,
    source_config: SourceConfig,
    creds: Credentials,
) -> None:
    """
    This loads data into the KWB data warehouse, hosted in a postgres db.

    Args:
        data: polars.DataFrame, data to be loaded into warehouse
        etl_config: ETLConfig, general variables for the etl process
        source_config: LayerConfig, parameters for the specific data type being processed
        creds: Credentials, dictionary of credentials used for ETL
    Returns:
        None
    """
    try:
        with get_pg_connection(etl_config.db_name, creds) as con:
            if not validate_table_exists(
                con, etl_config.schema_name, source_config.table_name
            ):
                raise LoadError(
                    f"Load Error for {source_config.name}: Target table does not exist in schema"
                )

            data_tuples = data.rows()

            col_names = sql.SQL(", ").join(
                sql.Identifier(col) for col in source_config.db_columns()
            )
            prim_key_cols = sql.SQL(" , ").join(
                sql.Identifier(col) for col in source_config.prim_key
            )
            update_cols = sql.SQL(" , ").join(
                sql.SQL(f"{col} = Excluded.{col}") for col in source_config.update_cols
            )
            upsert_query = sql.SQL(
                """
                INSERT INTO {schema_name}.{table} ({col_names}) 
                VALUES %s
                ON CONFLICT ({prim_key})
                DO UPDATE SET {update_cols}
                """
            ).format(
                schema_name=sql.Identifier(etl_config.schema_name),
                table=sql.Identifier(source_config.table_name),
                col_names=col_names,
                prim_key=prim_key_cols,
                update_cols=update_cols,
            )

            with con.cursor() as cur:
                execute_values(
                    cur, upsert_query, data_tuples, template=None, page_size=1000
                )
            logger.info(
                f"[{source_config.name}] Successfully loaded {data.height} rows"
            )
    except pg.OperationalError as e:
        raise LoadError(f"[{source_config.name}] Load Error: {e}")
    except Exception as e:
        raise LoadError(f"[{source_config.name}] Load Error: {e}")


def save_metrics(
    metrics: ETLMetrics,
    etl_config: ETLConfig,
    creds: Credentials,
):
    try:
        with get_pg_connection(etl_config.db_name, creds) as con:
            with con.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO logs.unified_loader_run_metrics 
                    ( run_id, run_start, run_end, source_name,
                    source_start, source_end, records_extracted, records_loaded,
                    status, error_message)
                    VALUES %s
                    """,
                    metrics.to_rows(),
                )
    except Exception as e:
        logger.error(f"Failed to load metrics to db: {e}")
