import polars, logging
from typing import Optional, Dict
from .models import SourceConfig

logger = logging.getLogger("agol_etl.transform")


class TransformError(Exception):
    pass


def transform(
    rawdata: polars.DataFrame, source_config: SourceConfig
) -> polars.DataFrame:
    """
    This transforms json data into parquets with correct data format.

    Args:
        source_config: dict, dictionary of parameters used for specific layer query
    Returns:
        result: polars.DataFrame, dataframe with data from AGOL layer layer
    """
    try:
        result = rawdata.with_columns(
            polars.from_epoch("start", time_unit="s")
            .dt.convert_time_zone("America/Los_Angeles")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )
        result = result.rename(source_config.column_mapping()).select(
            source_config.db_columns()
        )

        _validate_data(result, source_config)
        logger.info(
            f"[{source_config.name}] Successfully transformed {result.height} rows"
        )
        return result
    except polars.exceptions.ComputeError as e:
        raise TransformError(
            f"[{source_config.name}] Transformation Failed due to polars error: {e}"
        )
    except Exception as e:
        raise TransformError(f"[{source_config.name}] Transformation Failed: {e}")


def _validate_data(data: polars.DataFrame, source_config: SourceConfig):
    for col in source_config.prim_key:
        if col and data[col].null_count() > 0:
            raise TransformError(
                f"[{source_config.name}] Primary key column contains null values"
            )
