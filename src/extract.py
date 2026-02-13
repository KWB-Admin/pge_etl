import logging, requests, datetime, polars, boto3, json
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util import Retry
from typing import Dict
import xml.etree.ElementTree as ET
from .models import Credentials, SourceConfig, S3Config
from .exceptions import ExtractError

logger = logging.getLogger("pge_etl.extract")

ns = {"atom": "http://www.w3.org/2005/Atom", "espi": "http://naesb.org/espi"}


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


def build_schema(source_config) -> Dict:
    """
    Builds schema based on source_config input in etl_varibales.yaml
    Returns:
        schema: dict, dictionary with polars data types for each column
    """
    schema = source_config.field_schema()
    for key, value in schema.items():
        if value == "string":
            schema[key] = polars.String
        elif value == "int64":
            schema[key] = polars.Int64
        elif value == "float64":
            schema[key] = polars.Float64
        else:
            schema[key] = polars.String
    return schema


def extract(creds: Credentials, source_config: SourceConfig, s3config: S3Config):
    session = create_session()
    schema = build_schema(source_config)
    token = get_access_token(session, creds)
    data_list = []
    try:
        for urls in get_pending_webhooks(s3config.bucket, s3config.webhook_prefix):
            for url in urls:
                data_file = get_data(session, url, token)
                data_list.extend(parse_xml(data_file))
        data = polars.from_dicts(data_list, schema=schema)
        return data
    except Exception as e:
        raise ExtractError(str(e))


def get_pending_webhooks(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            response = s3.get_object(Bucket=bucket, Key=key)
            payload = json.loads(response["Body"].read().decode("utf-8"))
            url = payload.get("urls")
            yield url


def get_access_token(session: requests.Session, creds: Credentials) -> str:
    url = (
        "https://api.pge.com/datacustodian/oauth/v2/token?grant_type=client_credentials"
    )

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = session.post(
            url=url,
            headers=headers,
            auth=HTTPBasicAuth(creds.client_id, creds.client_secret),
            cert=("cert/certificate.txt", "cert/private_key_1.txt"),
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise ExtractError(f"Error returned: {data['error']}")
        return data["client_access_token"]
    except requests.Timeout as e:
        raise ExtractError(f"Request timed out: {e}")
    except requests.RequestException as e:
        raise ExtractError(f"Request failed: {e}")


def get_data(session: requests.Session, url: str, token: str) -> Dict:
    header = {"Authorization": f"Bearer {token}"}
    try:
        response = session.get(
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
        raise ExtractError(f"Request timed out: {e}")
    except requests.RequestException as e:
        raise ExtractError(f"Request failed: {e}")


def parse_xml(xml_file):
    """
    Parses a simple XML file and returns a list of dictionaries.
    Assumes a structure like: <root> <row> <col1>data</col1> ... </row> </root>
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    data_list = []

    for child in root.findall("atom:entry", ns):
        content = child.find("atom:content", ns)
        data = content.find(".//espi:IntervalBlock", ns)
        if data is None:
            continue
        usage_point_id = extract_usage_point_id(child)
        for reading in data.findall("espi:IntervalReading", ns):
            row = {
                "usage_point": usage_point_id,
                "reading_quality": reading.findtext(
                    "espi:ReadingQuality/espi:quality", default=None, namespaces=ns
                ),
                "duration": reading.findtext(
                    "espi:timePeriod/espi:duration", default=None, namespaces=ns
                ),
                "start": reading.findtext(
                    "espi:timePeriod/espi:start", default=None, namespaces=ns
                ),
                "value": reading.findtext("espi:value", default=None, namespaces=ns),
                "tou": reading.findtext("espi:tou", default=None, namespaces=ns),
                "unit": "kWh",
            }
            data_list.append(row)
    return data_list


def extract_usage_point_id(child):
    for link in child.findall("atom:link", ns):
        if link.get("rel") == "up":
            ref_split = link.get("href").split("/")
            return ref_split[(ref_split.index("UsagePoint") + 1)]
        else:
            continue
