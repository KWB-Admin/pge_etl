from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class SourceMetrics:
    source_name: str
    source_start: datetime = None
    source_end: datetime = None
    records_extracted: int = 0
    records_uploaded: int = 0
    status: str = "pending"
    error_msg: Optional[str] = None

    def duration_seconds(self) -> Optional[float]:
        if self.source_start and self.source_end:
            return (self.source_end - self.source_start).total_seconds()
        return None


@dataclass
class ETLMetrics:
    run_id: str
    run_start: datetime
    run_end: datetime = None
    sources: Dict[str, SourceMetrics] = field(default_factory=dict)

    def start_source(self, source_name: str):
        metrics = SourceMetrics(source_name=source_name, source_start=datetime.now())
        self.sources[source_name] = metrics
        return metrics

    def duration_seconds(self) -> Optional[float]:
        if self.run_start and self.run_end:
            return (self.run_end - self.run_start).total_seconds()
        return None

    def total_extracted(self):
        return sum([m.records_extracted for m in self.sources.values()])

    def total_uploaded(self):
        return sum([m.records_uploaded for m in self.sources.values()])

    def failed_sources(self):
        return [m.source_name for m in self.sources.values() if m.status == "failed"]

    def to_rows(self):
        return [
            (
                self.run_id,
                self.run_start,
                self.run_end,
                m.source_name,
                m.source_start,
                m.source_end,
                m.records_extracted,
                m.records_uploaded,
                m.status,
                m.error_msg,
            )
            for m in self.sources.values()
        ]


@dataclass
class FieldMapping:
    json_field: str
    db_field: str
    dtype: str


@dataclass
class SourceConfig:
    name: str
    table_name: str
    prim_key: List[str]
    update_cols: List[str]
    schema: List[FieldMapping]

    def field_schema(self) -> Dict[str, str]:
        return {f.json_field: f.dtype for f in self.schema}

    def db_columns(self) -> List[str]:
        return [f.db_field for f in self.schema]

    def column_mapping(self) -> Dict[str, str]:
        return {f.json_field: f.db_field for f in self.schema}

    def get_json_field(self, db_field: str) -> Optional[str]:
        for mapping in self.schema:
            if mapping.db_field == db_field:
                return mapping.json_field
        return None

    def validate(self) -> List[str]:
        errors = []
        db_cols = self.db_columns()
        for col in self.prim_key:
            if col not in db_cols:
                errors.append(f"[{self.name}] primary key col {col} not in db schema")
        for col in self.update_cols:
            if col not in db_cols:
                errors.append(f"[{self.name}] update col {col} not in db schema")

        return errors


@dataclass
class ETLConfig:
    db_name: str
    schema_name: str
    sources: Dict[str, SourceConfig]

    def validate(self) -> List[str]:
        errors = []
        if not self.db_name:
            errors.append("db_name is required")
        if not self.schema_name:
            errors.append("schema_name is required")
        if not self.sources:
            errors.append("layers is required")
        for layer in self.sources.values():
            errors.extend(layer.validate())

        return errors


@dataclass
class Credentials:
    client_id: str
    client_secret: str
    user: str
    host: str
    password: str

    def validate(self) -> List[str]:
        errors = []
        required = ["client_id", "client_secret", "user", "host", "password"]
        for field_name in required:
            if not getattr(self, field_name, None):
                errors.append(f"credentials.{field_name} not in credential file.")

        return errors
