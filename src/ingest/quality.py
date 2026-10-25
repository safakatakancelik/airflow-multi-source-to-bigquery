"""Row-count and key-integrity checks comparing source against what landed in BigQuery."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass
class QualityReport:
    rows_extracted: int
    rows_transformed: int
    rows_staged: int
    null_key_rows: int | None
    duplicate_key_groups: int | None
    failures: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        return "failed" if self.failures else "passed"

    def as_dict(self) -> dict:
        return {**asdict(self), "status": self.status}


def evaluate(
    *,
    rows_extracted: int,
    rows_transformed: int,
    rows_staged: int,
    null_key_rows: int | None,
    duplicate_key_groups: int | None,
) -> QualityReport:
    report = QualityReport(
        rows_extracted, rows_transformed, rows_staged, null_key_rows, duplicate_key_groups
    )
    if rows_transformed != rows_extracted:
        report.failures.append(
            f"transform changed row count: extracted={rows_extracted} transformed={rows_transformed}"
        )
    if rows_staged != rows_transformed:
        report.failures.append(
            f"row count mismatch: transformed={rows_transformed} staged_in_bigquery={rows_staged}"
        )
    if null_key_rows:
        report.failures.append(f"{null_key_rows} row(s) have a NULL key")
    if duplicate_key_groups:
        report.failures.append(f"{duplicate_key_groups} duplicated key value(s)")
    return report
