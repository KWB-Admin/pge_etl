import logging
from uuid import uuid4
from datetime import datetime
from .models import ETLMetrics
from .config import load_config, load_credentials
from .extract import extract, create_session
from .transform import transform
from .load import bulk_load_data, save_metrics
from .exceptions import ConfigError, ExtractError, TransformError, LoadError

# Logging Config
logging.basicConfig(
    filename="log/pge_etl.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)

logger = logging.getLogger("pge_etl")


def main():
    try:
        creds = load_credentials("config/credentials.yaml")
        config = load_config("config/etl_variables.yaml")
    except ConfigError as e:
        logger.error(f"Error while loading credentials/config: {e}")
        return None
    start_time = datetime.now()
    logger.info("============== PGE ETL started at %s ==============" % (start_time))
    metrics = ETLMetrics(run_id=str(uuid4()), run_start=start_time)
    for source_name, source_config in config.sources.items():
        source_metrics = metrics.start_source(source_name=source_name)
        logger.debug(f"[{source_name}] Running ETL")
        try:
            rawdata = extract(
                creds=creds, source_config=source_config, s3config=config.s3
            )

            source_metrics.records_extracted = rawdata.height
            if source_metrics.records_extracted == 0:
                source_metrics.status = "skipped"
                source_metrics.source_end = datetime.now()
                continue

            processed_data = transform(rawdata=rawdata, source_config=source_config)

            bulk_load_data(
                data=processed_data,
                etl_config=config,
                source_config=source_config,
                creds=creds,
            )

            source_metrics.status = "success"
            source_metrics.records_uploaded = processed_data.height
        except (ExtractError, TransformError, LoadError) as e:
            source_metrics.status = "failed"
            source_metrics.error_msg = str(e)
            logger.error(str(e))
        finally:
            source_metrics.source_end = datetime.now()
    metrics.run_end = datetime.now()
    logger.info(
        f"ETL Completed in {metrics.duration_seconds()} seconds, "
        f"Successfully loaded {metrics.total_uploaded()}/{metrics.total_extracted()} rows, "
        f"with Failed Layers: {metrics.failed_sources()}"
    )
    save_metrics(metrics, config, creds)
    logger.info("============== PGE ETL finished ==============\n")


if __name__ == "__main__":
    main()
