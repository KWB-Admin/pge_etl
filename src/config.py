from yaml import load, SafeLoader, YAMLError
import logging
from .models import Credentials, SourceConfig, ETLConfig, FieldMapping, S3Config
from .exceptions import ConfigError

logger = logging.getLogger("pge_etl.config")


def load_config(path: str):
    try:
        with open(path, "r") as file:
            raw = load(file, SafeLoader)
    except FileNotFoundError as e:
        raise ConfigError(f"File does not exist at {path}: {e}")
    except YAMLError as e:
        raise ConfigError(f"Invalid Yaml at {path}: {e}")

    try:
        s3config = _build_s3_config(raw["s3"])
    except KeyError as e:
        raise ConfigError(f"Config missing required S3 key: {e}")

    sources = {}
    for source_data in raw["sources"]:
        try:
            sources[source_data["name"]] = _build_source_config(source_data)
        except KeyError as e:
            raise ConfigError(f"Source {source_data["name"]} missing required key: {e}")

    config = ETLConfig(
        db_name=raw["db_name"],
        schema_name=raw["schema_name"],
        s3=s3config,
        sources=sources,
    )

    errors = config.validate()
    if errors:
        error_msg = "Loading of ETL Config failed:\n " + "\n".join(errors)
        raise ConfigError(error_msg)
    return config


def _build_source_config(source_data: dict):
    schema = [
        FieldMapping(
            json_field=field["json_field"],
            db_field=field["db_field"],
            dtype=field["dtype"],
        )
        for field in source_data["schema"]
    ]
    return SourceConfig(
        name=source_data["name"],
        table_name=source_data["table_name"],
        prim_key=source_data["prim_key"],
        update_cols=source_data["update_cols"],
        schema=schema,
    )


def _build_s3_config(s3info: dict):
    return S3Config(
        bucket=s3info["bucket"],
        webhook_prefix=s3info["webhook_prefix"],
        archive_prefix=s3info["archive_prefix"],
    )


def load_credentials(path: str):
    try:
        with open(path, "r") as file:
            raw = load(file, SafeLoader)
    except FileNotFoundError as e:
        raise ConfigError(f"File does not exist at {path}: {e}")
    except YAMLError as e:
        raise ConfigError(f"Invalid Yaml at {path}: {e}")

    creds = Credentials(
        client_id=raw["client_id"],
        client_secret=raw["client_secret"],
        user=raw["user"],
        host=raw["host"],
        password=raw["password"],
    )

    errors = creds.validate()
    if errors:
        error_msg = "Loading of ETL Config failed:\n " + "\n".join(errors)
        raise ConfigError(error_msg)
    return creds
