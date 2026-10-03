from app.preprocess import approx_tokens, mask, signal_line, split_lines, window

AIRFLOW_LOG = """\
[2026-09-27T00:34:40.716+0000] {taskinstance.py:2867} INFO - Starting attempt 1 of 1
[2026-09-27T00:34:40.912+0000] {taskinstance.py:732} INFO - ::endgroup::
[2026-09-27T00:34:40.930+0000] {taskinstance.py:3313} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/requests/adapters.py", line 667, in send
    resp = conn.urlopen(
           ^^^^^^^^^^^^^
urllib3.exceptions.MaxRetryError: HTTPSConnectionPool(host='api.partner.io', port=443)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/opt/airflow/dags/sync_partner.py", line 41, in pull
    raise ValueError(f"partner sync failed: {exc}")
ValueError: partner sync failed: Max retries exceeded
[2026-09-27T00:34:40.950+0000] {taskinstance.py:1225} INFO - Marking task as FAILED.
[2026-09-27T00:34:41.001+0000] {local_task_job_runner.py:266} INFO - Task exited with return code 1
"""


def test_strips_ansi_and_airflow_prefixes() -> None:
    lines = split_lines("\x1b[31m[2026-09-27T00:34:40.930+0000] {ti.py:1} ERROR - boom\x1b[0m")
    assert lines[0].text == "boom"
    assert lines[0].level == "ERROR"
    assert lines[0].prefixed
    plain = split_lines("[2026-09-27 00:34:40] WARNING - careful\n  continuation")
    assert (plain[0].text, plain[0].level) == ("careful", "WARNING")
    assert not plain[1].prefixed


def test_masks_volatile_tokens() -> None:
    text = mask(
        "run 3f2a9c1e-1b2c-4d5e-8f90-a1b2c3d4e5f6 host 10.0.12.7:5432 id deadbeef42 "
        "pid 123456 file /home/airflow/.local/lib/site-packages/requests/adapters.py code 503"
    )
    assert "<uuid>" in text and "<ip>" in text and "<hex>" in text and "<num>" in text
    assert "<path>/requests/adapters.py" in text
    assert "503" in text  # short numbers (HTTP/exit codes) carry signal


def test_window_keeps_chained_traceback_and_signal_line() -> None:
    w = window(AIRFLOW_LOG)
    assert "MaxRetryError" in w.text  # the buried root cause survives
    assert "ValueError: partner sync failed" in w.text
    assert "^^^^" not in w.text
    assert w.signal_line == "ValueError: partner sync failed: Max retries exceeded"
    assert w.text.index("MaxRetryError") < w.text.index("ValueError")  # original order


def test_window_respects_budget_and_prefers_errors() -> None:
    noise = "\n".join(f"[2026-01-01 00:00:00] INFO - processed batch {i} rows ok" for i in range(5000))
    log = noise[: len(noise) // 2] + "\npsycopg2.errors.UniqueViolation: duplicate key\n" + noise
    w = window(log, max_tokens=120)
    assert w.tokens <= 120
    assert approx_tokens(w.text) <= 120
    assert "UniqueViolation" in w.text
    assert w.signal_line == "psycopg2.errors.UniqueViolation: duplicate key"


def test_window_dedupes_repeated_lines() -> None:
    log = "\n".join(["[2026-01-01 00:00:00] ERROR - retrying connection to db-7"] * 50)
    w = window(log)
    assert w.lines == 1


def test_empty_and_errorless_logs() -> None:
    assert window("").text == ""
    assert window(None).signal_line is None
    w = window("[2026-01-01 00:00:00] INFO - Task exited with return code 1")
    assert w.signal_line == "Task exited with return code 1"


def test_signal_line_prefers_exception_over_error_record() -> None:
    lines = split_lines(
        "[2026-01-01 00:00:00] ERROR - Task failed with exception\nKeyError: 'customer_id'"
    )
    assert signal_line(lines) == "KeyError: 'customer_id'"
