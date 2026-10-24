from ingest.quality import evaluate


def ok(**over):
    base = dict(
        rows_extracted=10,
        rows_transformed=10,
        rows_staged=10,
        null_key_rows=0,
        duplicate_key_groups=0,
        allow_empty=False,
    )
    return evaluate(**{**base, **over})


def test_clean_run_passes():
    assert ok().status == "passed" and not ok().failures


def test_row_count_mismatch_fails():
    report = ok(rows_staged=9)
    assert report.status == "failed" and "mismatch" in report.failures[0]


def test_transform_changing_row_count_fails():
    assert ok(rows_transformed=8, rows_staged=8).status == "failed"


def test_null_and_duplicate_keys_fail():
    assert len(ok(null_key_rows=2, duplicate_key_groups=1).failures) == 2


def test_no_key_configured_skips_key_checks():
    assert ok(null_key_rows=None, duplicate_key_groups=None).status == "passed"


def test_empty_source_fails_unless_allowed():
    empty = dict(rows_extracted=0, rows_transformed=0, rows_staged=0)
    assert ok(**empty).status == "failed"
    assert ok(**empty, allow_empty=True).status == "passed"
