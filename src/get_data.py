import requests
from requests.adapters import HTTPAdapter
from get_token import get_access_token
from urllib3.util import Retry
from typing import Dict
from yaml import load, SafeLoader
import polars as pl
from parse_usage_data import parse_xml
import datetime

schema = {
    "usage_point": pl.String,
    "reading_quality": pl.String,
    "duration": pl.Int64,
    "start": pl.Int64,
    "value": pl.Float64,
    "tou": pl.String,
}


def create_session() -> requests.Session:
    """Creates a request session with predetermined retry logic"""
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    return session


def get_data(url, token) -> Dict:
    header = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(
            url=url,
            headers=header,
            cert=("cert/certificate.txt", "cert/private_key_1.txt"),
            timeout=30,
        )
        response.raise_for_status()
        filename = f"data/api_response_{datetime.date.today()}.xml"
        with open(filename, "w") as f:
            f.write(response.text)
        if "error" in response:
            raise Exception(f"Error returned: {response['error']}")
        return filename
    except requests.Timeout as e:
        raise Exception(f"Request timed out: {e}")
    except requests.RequestException as e:
        raise Exception(f"Request failed: {e}")


if __name__ == "__main__":
    with open("config/credentials.yaml", "r") as file:
        creds = load(file, SafeLoader)

    url = ""
    data = get_access_token(creds["client_id"], creds["client_secret"])
    token = data["client_access_token"]
    data_file = get_data(url, token)

    data_list = parse_xml(data_file)
    data = pl.from_dicts(data_list, schema=schema)
    data = data.with_columns(
        pl.from_epoch("start", time_unit="s")
        .dt.convert_time_zone("America/Los_Angeles")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .alias("start_time"),
        pl.lit("kWh").alias("unit"),
    )
    data.write_csv(f"processed_data/data_{datetime.date.today()}.csv")
