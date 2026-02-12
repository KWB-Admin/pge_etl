from yaml import load, SafeLoader, YAMLError
import logging
from .models import Credentials, SourceConfig, ETLConfig, FieldMapping
from .exceptions import ConfigError

logger = logging.getLogger("agol_etl.config")


def load_config(path: str):
    try:
        with open(path, "r") as file:
            raw = load(file, SafeLoader)
    except FileNotFoundError as e:
        raise ConfigError(f"File does not exist at {path}: {e}")
    except YAMLError as e:
        raise ConfigError(f"Invalid Yaml at {path}: {e}")

    sources = {}
    for source_data in raw["sources"]:
        try:
            sources[source_data["name"]] = _build_source_config(source_data)
        except KeyError as e:
            raise ConfigError(f"Layer {source_data["name"]} missing required key: {e}")

    config = ETLConfig(
        db_name=raw["db_name"],
        schema_name=raw["schema_name"],
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
