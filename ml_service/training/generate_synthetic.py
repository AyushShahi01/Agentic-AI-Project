"""Generate synthetic labeled task logs from hand-written templates per category.

Templates are realistic error messages from the libraries Airflow tasks use (DB drivers, cloud
SDKs, HTTP clients, Spark, dbt) — NOT generated from the regex classifier's patterns, so the
model can learn causes regex misses. Each sample is wrapped in Airflow 2/3 log boilerplate with
noise, and a share are "hard" cases:

* ``reraise``  — the root cause re-raised as a generic ValueError/RuntimeError/AirflowException
  (e.g. a ValueError raised inside a network or auth library);
* ``chained``  — nested tracebacks where the cause is the inner exception;
* ``buried``   — the cause is a single ERROR line among lots of INFO noise;
* ``custom``   — the cause's message under an in-house exception class (``etl.errors.LoadError``),
  so the class name alone never decides the category.

Every sample also gets *distractor* lines: INFO/WARNING lines that mention other categories'
words (a recovered connection retry, a token refresh, memory usage) without being the cause, and
chained samples may end in a built-in TypeError/KeyError raised while handling the real cause.
Both stop the model from keyword-matching or calling every built-in exception a code bug.

Every sample records its ``template_id`` so ``train.py`` can split by template (no leakage).

Usage:  python -m training.generate_synthetic [--n 6300] [--out data/synthetic.jsonl]
"""

import argparse
import json
import random
from collections.abc import Callable
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "data"

R = random.Random()


def _pick(*xs: str) -> Callable[[], str]:
    return lambda: R.choice(xs)


SLOTS: dict[str, Callable[[], str]] = {
    "table": _pick("orders", "customers", "events", "payments", "invoices", "sessions", "fx_rates",
                   "inventory", "shipments", "subscriptions", "ledger_entries", "clicks"),
    "schema": _pick("staging", "raw", "analytics", "mart", "public", "dw", "bronze", "silver"),
    "col": _pick("customer_id", "amount", "created_at", "order_id", "email", "region",
                 "unit_price", "currency", "status", "sku", "event_ts", "country_code"),
    "user": _pick("etl", "airflow", "loader", "svc_ingest", "reporting", "dbt_runner"),
    "host": _pick("db-prod-1.internal", "warehouse.acme.io", "10.12.4.21", "pg-replica-2",
                  "mysql-main.svc.cluster.local", "api.vendor.com", "sftp.partner.net"),
    "url": _pick("https://api.vendor.com/v2/export", "https://graph.partner.io/orders",
                 "https://sheets.googleapis.com/v4/spreadsheets/abc", "https://hooks.acme.io/in",
                 "https://login.microsoftonline.com/tenant/oauth2/v2.0/token"),
    "bucket": _pick("acme-landing", "lake-raw", "exports-prod", "vendor-drop", "dwh-stage"),
    "date": lambda: f"2026-{R.randint(1, 12):02d}-{R.randint(1, 28):02d}",
    "n": lambda: str(R.randint(2, 99999)),
    "small": lambda: str(R.randint(2, 64)),
    "secs": lambda: str(R.choice([30, 60, 120, 300, 600, 900, 1800, 3600, 7200])),
    "gb": lambda: f"{R.randint(1, 64)}.{R.randint(0, 9)}",
    "pid": lambda: str(R.randint(1000, 99999)),
    "port": _pick("5432", "3306", "443", "1433", "9092", "6379"),
    "dag": _pick("ingest_orders", "daily_finance", "crm_sync", "ml_features", "events_rollup",
                 "partner_export", "billing_close"),
    "task": _pick("extract", "load", "transform", "publish", "validate", "sync", "aggregate"),
    "file": _pick("orders.csv", "events.parquet", "customers.json", "export.csv.gz",
                  "ledger.avro", "manifest.json"),
    "func": _pick("transform_batch", "load_rows", "build_payload", "normalize", "merge_frames"),
    "var": _pick("df", "rows", "payload", "conn", "result", "batch", "cfg"),
}


def fill(template: str) -> str:
    out = template
    for key, gen in SLOTS.items():
        token = "{" + key + "}"
        while token in out:
            out = out.replace(token, gen(), 1)
    return out


# category -> list of templates. Multi-line templates keep their lines.
TEMPLATES: dict[str, list[str]] = {
    "AUTH": [
        "psycopg2.OperationalError: connection to server at \"{host}\", port {port} failed: "
        "FATAL:  password authentication failed for user \"{user}\"",
        "snowflake.connector.errors.DatabaseError: 250001 (08001): Failed to connect to DB. "
        "Incorrect username or password was specified.",
        "google.auth.exceptions.RefreshError: ('invalid_grant: Bad Request', {'error': "
        "'invalid_grant', 'error_description': 'Bad Request'})",
        "google.auth.exceptions.DefaultCredentialsError: Could not automatically determine "
        "credentials. Please set GOOGLE_APPLICATION_CREDENTIALS",
        "botocore.exceptions.ClientError: An error occurred (ExpiredToken) when calling the "
        "AssumeRole operation: The security token included in the request is expired",
        "botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the "
        "GetObject operation: Access Denied",
        "botocore.exceptions.NoCredentialsError: Unable to locate credentials",
        "botocore.exceptions.ClientError: An error occurred (UnrecognizedClientException) when "
        "calling the PutRecord operation: The security token included in the request is invalid.",
        "azure.core.exceptions.ClientAuthenticationError: Authentication failed: AADSTS7000215: "
        "Invalid client secret provided.",
        "requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: {url}",
        "requests.exceptions.HTTPError: 403 Client Error: Forbidden for url: {url}",
        "ValueError: API request to {url} failed (401): {\"message\": \"Bad credentials\"}",
        "ValueError: token refresh failed: server replied invalid_client",
        "RuntimeError: Vendor API rejected our credentials: HTTP 401 invalid_token",
        "pymysql.err.OperationalError: (1045, \"Access denied for user '{user}'@'{host}' "
        "(using password: YES)\")",
        "pyodbc.InterfaceError: ('28000', \"[28000] [Microsoft][ODBC Driver 18 for SQL Server]"
        "[SQL Server]Login failed for user '{user}'. (18456)\")",
        "paramiko.ssh_exception.AuthenticationException: Authentication failed.",
        "paramiko.ssh_exception.SSHException: No authentication methods available",
        "jwt.exceptions.ExpiredSignatureError: Signature has expired",
        "slack_sdk.errors.SlackApiError: The request to the Slack API failed.\nThe server "
        "responded with: {'ok': False, 'error': 'token_revoked'}",
        "google.api_core.exceptions.Forbidden: 403 GET {url}: Caller does not have permission",
        "snowflake.connector.errors.ProgrammingError: 390144 (08004): JWT token is invalid.",
        "hvac.exceptions.Forbidden: permission denied, on get https://vault:8200/v1/secret/{user}",
        "kubernetes.client.exceptions.ApiException: (403)\nReason: Forbidden\nHTTP response body: "
        "pods is forbidden: User \"system:serviceaccount:etl:{user}\" cannot list resource",
        "psycopg2.OperationalError: FATAL:  no pg_hba.conf entry for host \"{host}\", user "
        "\"{user}\", database \"warehouse\", SSL on",
        "airflow.exceptions.AirflowException: HTTP error: 401 Unauthorized, invalid api key",
        "ValueError: OAuth2 token endpoint returned error=unauthorized_client",
        "msal: AADSTS50126: Error validating credentials due to invalid username or password.",
        "google.auth.exceptions.RefreshError: ('invalid_grant: Token has been expired or revoked.')",
        "simple_salesforce.exceptions.SalesforceAuthenticationFailed: INVALID_LOGIN: Invalid "
        "username, password, security token; or user locked out.",
        "oauthlib.oauth2.rfc6749.errors.TokenExpiredError: (token_expired)",
        "PermissionError: [Errno 13] Permission denied: '/data/inbound/{file}'",
        "botocore.exceptions.ClientError: An error occurred (InvalidAccessKeyId) when calling the "
        "ListObjectsV2 operation: The AWS Access Key Id you provided does not exist in our records.",
        "databricks.sdk.errors.platform.Unauthenticated: Invalid access token.",
        "RuntimeError: login to {url} returned 'session expired, please re-authenticate'",
        "ValueError: response from {url}: {\"error\": \"invalid_api_key\", \"status\": 401}",
    ],
    "DATA_INTEGRITY": [
        "psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint "
        "\"{table}_pkey\"\nDETAIL:  Key ({col})=({n}) already exists.",
        "sqlalchemy.exc.IntegrityError: (psycopg2.errors.NotNullViolation) null value in column "
        "\"{col}\" of relation \"{table}\" violates not-null constraint",
        "psycopg2.errors.ForeignKeyViolation: insert or update on table \"{table}\" violates "
        "foreign key constraint \"{table}_{col}_fkey\"",
        "psycopg2.errors.InvalidTextRepresentation: invalid input syntax for type numeric: "
        "\"n/a\"",
        "psycopg2.errors.InvalidDatetimeFormat: invalid input syntax for type date: \"31/02/2026\"",
        "psycopg2.errors.StringDataRightTruncation: value too long for type character varying"
        "({small})",
        "psycopg2.errors.NumericValueOutOfRange: integer out of range",
        "pymysql.err.IntegrityError: (1062, \"Duplicate entry '{n}' for key 'PRIMARY'\")",
        "pymysql.err.DataError: (1406, \"Data too long for column '{col}' at row {n}\")",
        "snowflake.connector.errors.ProgrammingError: 100038 (22018): Numeric value 'abc' is not "
        "recognized",
        "snowflake.connector.errors.ProgrammingError: 100040 (22007): Date '{n}' is not "
        "recognized",
        "google.api_core.exceptions.BadRequest: 400 Error while reading data, error message: "
        "Could not parse '{n}x' as INT64 for field {col}",
        "ValueError: invalid literal for int() with base 10: 'N/A' (row {n} of {file})",
        "ValueError: could not convert string to float: '1,234.5' in column {col}",
        "ValueError: {n} duplicate {col} values found in {schema}.{table}; refusing to load",
        "AssertionError: null check failed: {n} rows with empty {col}",
        "airflow.exceptions.AirflowException: Data quality check failed for {schema}.{table}: "
        "{col} has {n} negative values",
        "airflow.providers.common.sql.operators.sql.SQLCheckOperator: Test failed.\nQuery:\n"
        "SELECT COUNT(*) FROM {table} WHERE {col} IS NULL\nResults:\n(0,)",
        "dbt: Failure in test not_null_{table}_{col} (models/staging/schema.yml)\n  Got {small} "
        "results, configured to fail if != 0",
        "dbt: Failure in test accepted_values_{table}_{col}\n  Got {small} results",
        "great_expectations: Validation of {table} failed: expect_column_values_to_be_unique "
        "({col}) unexpected_count={n}",
        "pandas.errors.ParserError: Error tokenizing data. C error: Expected {small} fields in "
        "line {n}, saw {small}",
        "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe9 in position {n}: invalid "
        "continuation byte",
        "pyarrow.lib.ArrowInvalid: Could not convert '{date}T25:61' with type str: tried to "
        "convert to timestamp",
        "ValueError: unconverted data remains: T00:00:00Z (column {col})",
        "RuntimeError: reconciliation failed: source total {n} != target total {n}",
        "json.decoder.JSONDecodeError: Expecting value: line {small} column {small} (char {n}) "
        "while parsing record {n} of {file}",
        "ValueError: time data '{date}T99' does not match format '%Y-%m-%d' (column {col})",
        "decimal.InvalidOperation: [<class 'decimal.ConversionSyntax'>] while parsing {col} in "
        "row {n}",
        "OverflowError: Python int too large to convert to C long (column {col}, row {n})",
        "pydantic_core._pydantic_core.ValidationError: 1 validation error for Record\n{col}\n  "
        "Input should be a valid integer, unable to parse string as an integer",
        "psycopg2.errors.CheckViolation: new row for relation \"{table}\" violates check "
        "constraint \"{table}_{col}_positive\"",
        "pandera.errors.SchemaError: Column '{col}' failed element-wise validator number 0: "
        "greater_than(0) failure cases: -{small}",
        "TypeError: '<' not supported between instances of 'str' and 'float' (mixed values in "
        "column {col})",
    ],
    "SCHEMA": [
        "psycopg2.errors.UndefinedColumn: column \"{col}\" does not exist\nLINE 1: SELECT {col}, "
        "id FROM {schema}.{table}",
        "psycopg2.errors.UndefinedTable: relation \"{schema}.{table}\" does not exist",
        "sqlite3.OperationalError: no such column: {col}",
        "pymysql.err.OperationalError: (1054, \"Unknown column '{col}' in 'field list'\")",
        "pymysql.err.ProgrammingError: (1146, \"Table '{schema}.{table}' doesn't exist\")",
        "snowflake.connector.errors.ProgrammingError: 000904 (42000): SQL compilation error: "
        "invalid identifier '{col}'",
        "snowflake.connector.errors.ProgrammingError: 002003 (42S02): SQL compilation error: "
        "Object '{schema}.{table}' does not exist or not authorized.",
        "google.api_core.exceptions.BadRequest: 400 Unrecognized name: {col} at [{small}:{small}]",
        "google.api_core.exceptions.BadRequest: 400 Provided Schema does not match Table "
        "{schema}.{table}. Cannot add fields (field: {col})",
        "google.api_core.exceptions.BadRequest: 400 Field {col} has changed type from STRING to "
        "INTEGER",
        "pyspark.errors.exceptions.captured.AnalysisException: [UNRESOLVED_COLUMN] A column or "
        "function parameter with name `{col}` cannot be resolved.",
        "pyspark.errors.exceptions.captured.AnalysisException: [CANNOT_MERGE_SCHEMAS] Failed "
        "merging schemas: field {col} type mismatch (LongType vs StringType)",
        "pyarrow.lib.ArrowInvalid: Schema at index {small} was different",
        "pyarrow.lib.ArrowTypeError: Expected bytes, got a 'int' object (column {col})",
        "KeyError: \"['{col}'] not in index\"",
        "KeyError: \"Column(s) ['{col}'] do not exist\"",
        "KeyError: '{col}' -- column missing from upstream extract (schema changed)",
        "ValueError: Length mismatch: Expected axis has {small} elements, new values have "
        "{small} elements",
        "ValueError: Usecols do not match columns, columns expected but not found: ['{col}']",
        "psycopg2.errors.DatatypeMismatch: column \"{col}\" is of type integer but expression is "
        "of type text",
        "psycopg2.errors.UndefinedFunction: operator does not exist: integer = text (column "
        "{col} changed type)",
        "duckdb.duckdb.BinderException: Binder Error: Referenced column \"{col}\" not found in "
        "FROM clause!",
        "dbt: Database Error in model {table} (models/marts/{table}.sql)\n  column \"{col}\" does "
        "not exist",
        "fastavro: SchemaResolutionError: field '{col}' missing from writer schema and has no "
        "default",
        "sqlalchemy.exc.NoSuchTableError: {table}",
        "jsonschema.exceptions.ValidationError: '{col}' is a required property (payload schema "
        "v{small} changed)",
        "ValueError: columns of {file} changed: expected {small} columns, got {small}",
        "sqlalchemy.exc.ProgrammingError: (pymysql.err.ProgrammingError) (1146, \"Table "
        "'{schema}.{table}' doesn't exist\")",
        "pyspark.errors.exceptions.captured.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The "
        "table or view `{schema}`.`{table}` cannot be found.",
        "TypeError: Cannot compare types 'ndarray(dtype=int64)' and 'str' ({col} arrived as text "
        "after the source changed)",
        "AttributeError: 'DataFrame' object has no attribute '{col}' (source renamed the field)",
    ],
    "CODE_BUG": [
        "NameError: name '{var}' is not defined",
        "AttributeError: 'NoneType' object has no attribute 'strip'",
        "AttributeError: 'DataFrame' object has no attribute 'append'",
        "AttributeError: module 'datetime' has no attribute 'now'",
        "TypeError: {func}() got an unexpected keyword argument 'ds'",
        "TypeError: {func}() missing 1 required positional argument: '{var}'",
        "TypeError: can only concatenate str (not \"int\") to str",
        "TypeError: 'NoneType' object is not subscriptable",
        "TypeError: unhashable type: 'list'",
        "KeyError: '{var}'",
        "IndexError: list index out of range",
        "IndexError: single positional indexer is out-of-bounds",
        "ZeroDivisionError: float division by zero",
        "ValueError: not enough values to unpack (expected 3, got 2)",
        "ValueError: The truth value of a Series is ambiguous. Use a.empty, a.bool(), a.item(), "
        "a.any() or a.all().",
        "UnboundLocalError: cannot access local variable '{var}' where it is not associated with "
        "a value",
        "RecursionError: maximum recursion depth exceeded in comparison",
        "SyntaxError: invalid syntax ({func}.py, line {small})",
        "IndentationError: expected an indented block after 'if' statement on line {small}",
        "ModuleNotFoundError: No module named '{func}'",
        "ImportError: cannot import name '{func}' from 'etl.utils'",
        "AssertionError: {func} returned unexpected type <class 'list'>",
        "NotImplementedError: {func} is not implemented for this backend",
        "jinja2.exceptions.UndefinedError: '{var}' is undefined",
        "jinja2.exceptions.TemplateSyntaxError: unexpected end of template",
        "airflow.exceptions.AirflowException: Invalid arguments were passed to PythonOperator "
        "(task_id: {task}).",
        "airflow.exceptions.DuplicateTaskIdFound: Task id '{task}' has already been added to the "
        "DAG",
        "airflow.exceptions.AirflowException: Bash command failed. The command returned a "
        "non-zero exit code 127.\n{func}.sh: line {small}: jq: command not found",
        "sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "
        "\"WHER\"",
        "sqlalchemy.exc.ArgumentError: Textual SQL expression should be explicitly declared as "
        "text()",
        "pandas.errors.InvalidIndexError: Reindexing only valid with uniquely valued Index "
        "objects",
        "TypeError: {func}() takes 2 positional arguments but 3 were given",
        "AttributeError: 'list' object has no attribute 'items'",
        "pickle.PicklingError: Can't pickle <function <lambda>>: attribute lookup failed",
    ],
    "RESOURCE": [
        "MemoryError",
        "MemoryError: Unable to allocate {gb} GiB for an array with shape ({n}, {small})",
        "Task exited with return code Negsignal.SIGKILL",
        "Task exited with return code -9",
        "Process exited with exit code 137 (OOMKilled)",
        "Pod {task}-{pid} terminated: reason=OOMKilled, exit code 137",
        "Pod was evicted: The node was low on resource: memory. Container base was using "
        "{gb}Gi, which exceeds its request of 2Gi.",
        "Pod was evicted: The node was low on resource: ephemeral-storage.",
        "OSError: [Errno 28] No space left on device: '/tmp/{file}'",
        "OSError: [Errno 122] Disk quota exceeded",
        "OSError: [Errno 24] Too many open files",
        "OSError: [Errno 12] Cannot allocate memory",
        "java.lang.OutOfMemoryError: GC overhead limit exceeded",
        "java.lang.OutOfMemoryError: Java heap space",
        "Container killed by YARN for exceeding memory limits. {gb} GB of {gb} GB physical memory "
        "used.",
        "org.apache.spark.SparkException: Job aborted due to stage failure: ExecutorLostFailure "
        "(executor {small} exited caused by one of the running tasks) Reason: Container killed "
        "on request. Exit code is 137",
        "torch.OutOfMemoryError: CUDA out of memory. Tried to allocate {gb} GiB",
        "psycopg2.errors.DiskFull: could not write to file \"pg_tblspc/{n}\": No space left on "
        "device",
        "psycopg2.errors.OutOfMemory: out of memory\nDETAIL:  Failed on request of size {n}.",
        "psycopg2.OperationalError: FATAL:  remaining connection slots are reserved for "
        "non-replication superuser connections",
        "psycopg2.OperationalError: FATAL:  sorry, too many clients already",
        "snowflake.connector.errors.ProgrammingError: 000606 (57P03): Warehouse cannot be resumed "
        "because resource monitor has exceeded its quota.",
        "google.api_core.exceptions.Forbidden: 403 Quota exceeded: Your project exceeded quota "
        "for concurrent queries.",
        "google.api_core.exceptions.ResourceExhausted: 429 Resources exceeded during query "
        "execution: Not enough resources for query planning",
        "botocore.errorfactory.LimitExceededException: Account limit of concurrent queries "
        "reached",
        "ValueError: worker {small} died: memory usage {gb} GB exceeded limit of {gb} GB",
        "RuntimeError: dask worker killed: Worker exceeded 95% memory budget. Restarting",
        "airflow.exceptions.AirflowException: Pod launching failed: 0/{small} nodes are "
        "available: {small} Insufficient memory, {small} Insufficient cpu.",
        "numpy.core._exceptions._ArrayMemoryError: Unable to allocate {gb} GiB for an array",
        "RuntimeError: DataLoader worker (pid {pid}) is killed by signal: Killed.",
        "ValueError: chunk could not be processed: worker restarted after exceeding its memory "
        "limit",
        "Pod {task}-{pid} failed: container exceeded ephemeral storage limit",
    ],
    "TIMEOUT": [
        "airflow.exceptions.AirflowTaskTimeout: Timeout, PID: {pid}",
        "airflow.exceptions.AirflowSensorTimeout: Sensor has timed out; run duration of {secs} "
        "seconds exceeds the specified timeout of {secs}.",
        "psycopg2.errors.QueryCanceled: canceling statement due to statement timeout",
        "psycopg2.errors.LockNotAvailable: canceling statement due to lock timeout",
        "snowflake.connector.errors.ProgrammingError: 000630 (57014): Statement reached its "
        "statement or warehouse timeout of {secs} second(s) and was canceled.",
        "google.api_core.exceptions.DeadlineExceeded: 504 Deadline Exceeded",
        "google.api_core.exceptions.RetryError: Deadline of {secs}.0s exceeded while calling "
        "target function",
        "requests.exceptions.ReadTimeout: HTTPSConnectionPool(host='{host}', port=443): Read "
        "timed out. (read timeout={secs})",
        "requests.exceptions.ConnectTimeout: HTTPSConnectionPool(host='{host}', port=443): Max "
        "retries exceeded (connect timeout={secs})",
        "httpx.ReadTimeout: The read operation timed out",
        "TimeoutError: [Errno 110] Connection timed out",
        "concurrent.futures._base.TimeoutError",
        "asyncio.exceptions.CancelledError -> TimeoutError: job {n} did not complete",
        "botocore.exceptions.WaiterError: Waiter QueryCompleted failed: Max attempts exceeded",
        "ValueError: job {n} still RUNNING after {secs} seconds, giving up",
        "RuntimeError: export did not finish within {secs}s (last status: IN_PROGRESS)",
        "airflow.exceptions.AirflowException: Job run {n} has not completed after {secs} "
        "seconds.",
        "airflow.exceptions.AirflowException: dbt Cloud job {n} did not finish within the "
        "configured timeout",
        "pymysql.err.OperationalError: (3024, 'Query execution was interrupted, maximum statement "
        "execution time exceeded')",
        "pyodbc.OperationalError: ('HYT00', '[HYT00] [Microsoft][ODBC Driver 18 for SQL Server]"
        "Query timeout expired (0) (SQLExecDirectW)')",
        "Task did not complete within its execution_timeout of {secs} seconds and was killed",
        "AssertionError: polling for EMR cluster j-{n} timed out after {small} minutes",
        "airflow.exceptions.AirflowException: SSH command timed out after {secs} seconds",
        "snowflake.connector.errors.OperationalError: 000604: Statement cancelled: exceeded "
        "STATEMENT_TIMEOUT_IN_SECONDS",
        "RuntimeError: query {n} still running after {secs}s; cancelled by watchdog",
        "ValueError: poll limit reached waiting for report {n} (status=PENDING)",
    ],
    "TRANSIENT_NETWORK": [
        "requests.exceptions.ConnectionError: HTTPSConnectionPool(host='{host}', port=443): Max "
        "retries exceeded with url: /v1 (Caused by NewConnectionError('Failed to establish a new "
        "connection: [Errno 111] Connection refused'))",
        "urllib3.exceptions.ProtocolError: ('Connection aborted.', ConnectionResetError(104, "
        "'Connection reset by peer'))",
        "ConnectionResetError: [Errno 104] Connection reset by peer",
        "BrokenPipeError: [Errno 32] Broken pipe",
        "socket.gaierror: [Errno -2] Name or service not known",
        "socket.gaierror: [Errno -3] Temporary failure in name resolution",
        "OSError: [Errno 113] No route to host",
        "OSError: [Errno 101] Network is unreachable",
        "psycopg2.OperationalError: server closed the connection unexpectedly\n\tThis probably "
        "means the server terminated abnormally",
        "psycopg2.OperationalError: could not connect to server: Connection refused\n\tIs the "
        "server running on host \"{host}\" and accepting TCP/IP connections on port {port}?",
        "psycopg2.OperationalError: SSL SYSCALL error: Connection reset by peer",
        "pymysql.err.OperationalError: (2013, 'Lost connection to MySQL server during query')",
        "pymysql.err.OperationalError: (2003, \"Can't connect to MySQL server on '{host}'\")",
        "requests.exceptions.HTTPError: 502 Server Error: Bad Gateway for url: {url}",
        "requests.exceptions.HTTPError: 503 Server Error: Service Unavailable for url: {url}",
        "requests.exceptions.HTTPError: 429 Client Error: Too Many Requests for url: {url}",
        "httpx.RemoteProtocolError: peer closed connection without sending complete message body",
        "httpx.ConnectError: [Errno 111] Connection refused",
        "aiohttp.client_exceptions.ClientConnectorError: Cannot connect to host {host}:443",
        "ssl.SSLEOFError: EOF occurred in violation of protocol (_ssl.c:2427)",
        "grpc._channel._InactiveRpcError: status = StatusCode.UNAVAILABLE details = \"Connection "
        "reset by peer\"",
        "google.api_core.exceptions.ServiceUnavailable: 503 The service is currently unavailable.",
        "botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: "
        "\"https://{bucket}.s3.amazonaws.com/\"",
        "botocore.exceptions.ClientError: An error occurred (SlowDown) when calling the PutObject "
        "operation: Please reduce your request rate.",
        "kafka.errors.KafkaConnectionError: Connection at {host}:{port} closed",
        "redis.exceptions.ConnectionError: Error 111 connecting to {host}:{port}. Connection "
        "refused.",
        "ValueError: empty reply from {host} (connection dropped mid-response)",
        "RuntimeError: upstream service returned 503; retry later",
        "ValueError: Remote end closed connection without response while calling {url}",
        "requests.exceptions.ChunkedEncodingError: ('Connection broken: IncompleteRead(0 bytes "
        "read)', IncompleteRead(0 bytes read))",
        "http.client.RemoteDisconnected: Remote end closed connection without response",
        "google.auth.exceptions.TransportError: HTTPSConnectionPool(host='oauth2.googleapis.com', "
        "port=443): Max retries exceeded (Caused by NewConnectionError: Connection refused)",
        "botocore.exceptions.ConnectionClosedError: Connection was closed before we received a "
        "valid response from endpoint URL",
        "snowflake.connector.errors.OperationalError: 250003: Failed to get the response. "
        "Hanging? method: post, url: {url}",
        "smtplib.SMTPServerDisconnected: Connection unexpectedly closed",
        "pika.exceptions.AMQPConnectionError: Connection to {host}:{port} failed",
        "TypeError: 'NoneType' object is not subscriptable (empty response after the connection "
        "to {host} dropped)",
    ],
    "UPSTREAM_MISSING": [
        "FileNotFoundError: [Errno 2] No such file or directory: '/data/inbound/{date}/{file}'",
        "botocore.errorfactory.NoSuchKey: An error occurred (NoSuchKey) when calling the "
        "GetObject operation: The specified key does not exist.",
        "botocore.errorfactory.NoSuchBucket: The specified bucket does not exist",
        "botocore.exceptions.ClientError: An error occurred (404) when calling the HeadObject "
        "operation: Not Found",
        "google.api_core.exceptions.NotFound: 404 No such object: {bucket}/{date}/{file}",
        "google.api_core.exceptions.NotFound: 404 Not found: Table {schema}.{table}_{n}",
        "pyspark.errors.exceptions.captured.AnalysisException: [PATH_NOT_FOUND] Path does not "
        "exist: s3a://{bucket}/{table}/dt={date}",
        "pandas.errors.EmptyDataError: No columns to parse from file",
        "ValueError: no files matched s3://{bucket}/{table}/{date}/*.parquet",
        "ValueError: input partition dt={date} is empty",
        "AssertionError: expected {small} files for {date}, found 0",
        "airflow.exceptions.AirflowException: upstream file {file} for {date} has not arrived",
        "airflow.exceptions.AirflowException: The external task {task} in DAG {dag} failed.",
        "airflow.exceptions.AirflowException: The external DAG {dag} failed.",
        "airflow.exceptions.AirflowException: Upstream dataset {schema}.{table} not updated "
        "since {date}",
        "RuntimeError: dependency {dag}.{task} has no successful run for {date}",
        "KeyError: 'Contents' (prefix {table}/{date}/ is empty)",
        "IOError: SFTP file /outbound/{file} not found on {host}",
        "snowflake.connector.errors.ProgrammingError: 091016 (22000): Remote file "
        "'s3://{bucket}/{file}' was not found.",
        "Partition {date} not found in table {schema}.{table}",
        "TypeError: 'NoneType' object is not iterable (manifest for {date} missing from "
        "{bucket})",
        "ValueError: vendor drop for {date} not found in sftp://{host}/outbound",
        "airflow.exceptions.AirflowException: no new data for {date}: partition {schema}.{table} "
        "has 0 rows",
        "KeyError: '{date}' (no export for this date in the vendor manifest)",
        "RuntimeError: waited for {file} from {dag}; the producing job has not run for {date}",
    ],
    "UNKNOWN": [
        "Task exited with return code 1",
        "airflow.exceptions.AirflowException: Bash command failed. The command returned a "
        "non-zero exit code 1.",
        "airflow.exceptions.AirflowException: Bash command failed. The command returned a "
        "non-zero exit code 2.",
        "airflow.exceptions.AirflowException: Task received SIGTERM signal",
        "airflow.exceptions.AirflowException: Pod {task}-{pid} returned a failure.",
        "subprocess.CalledProcessError: Command '['python', '{func}.py']' returned non-zero exit "
        "status 1.",
        "RuntimeError: {task} failed",
        "Exception: job failed",
        "airflow.exceptions.AirflowException: Glue job run failed: see CloudWatch for details",
        "airflow.exceptions.AirflowException: Databricks run {n} terminated with state FAILED",
        "airflow.exceptions.AirflowException: EMR step failed",
        "Marking task as FAILED.",
        "airflow.exceptions.AirflowFailException: {task} reported failure",
        "SystemExit: 1",
        "RuntimeError: unexpected error in {func}; see previous log lines",
        "subprocess.CalledProcessError: Command 'spark-submit --deploy-mode cluster {func}.py' "
        "returned non-zero exit status 1.",
        "airflow.exceptions.AirflowException: Fivetran sync for connector {task} failed",
        "airflow.exceptions.AirflowException: Airbyte job {n} failed",
        "airflow.exceptions.AirflowException: Batch job {n} failed: Essential container in task "
        "exited",
        "airflow.exceptions.AirflowException: Dataflow job {n} failed",
        "airflow.exceptions.AirflowException: Step Functions execution {n} ended with status FAILED",
        "airflow.exceptions.AirflowException: Kubernetes pod {task}-{pid} failed: Error",
        "etl.errors.PipelineError: batch {n} rejected",
        "RuntimeError: load of {schema}.{table} failed",
        "RuntimeError: {func} returned status=ERROR",
        "Exception: API call to {url} was not successful",
        "Exception: Something went wrong",
        "ValueError: step {task} did not succeed",
        "java.lang.RuntimeException: Job aborted.\n\tat org.apache.spark.sql.execution.datasources"
        ".FileFormatWriter$.write(FileFormatWriter.scala:{small})",
        "npm ERR! code ELIFECYCLE\nnpm ERR! errno 1\nnpm ERR! {func}@1.0.0 start: `node index.js`",
        "Error: Process completed with exit code 2.",
        "Task {task} failed: exit code 255",
        "RuntimeError: worker returned an error: {'code': 'INTERNAL', 'message': 'internal error'}",
        "dbt: Completed with 1 error and 0 warnings:\n  Runtime Error in model {table}",
        "airflow.exceptions.AirflowException: {dag}.{task} failed; check the job's own logs",
        "ERROR - Process terminated with code 3",
        "airflow.exceptions.AirflowException: Snowflake task {table}_refresh ended in state FAILED",
    ],
}

# Wrappers that hide the real cause behind a generic exception.
REWRAP = [
    "ValueError: {func} failed: {cause}",
    "RuntimeError: {task} step failed: {cause}",
    "airflow.exceptions.AirflowException: {cause}",
    "Exception: error during {task}: {cause}",
]
CHAIN = [
    "During handling of the above exception, another exception occurred:",
    "The above exception was the direct cause of the following exception:",
]
GENERIC_OUTER = [
    # built-in errors raised while handling the real cause: the cause is still the inner one
    "TypeError: 'NoneType' object is not subscriptable",
    "AttributeError: 'NoneType' object has no attribute 'json'",
    "KeyError: 'data'",
    "airflow.exceptions.AirflowException: Task {task} failed",
    "ValueError: failed to process batch {n}",
    "RuntimeError: {func} raised an error",
    "airflow.exceptions.AirflowFailException: giving up after {small} attempts",
]
FRAMES = [
    ("/home/airflow/.local/lib/python3.{small}/site-packages/airflow/models/taskinstance.py",
     "_execute_task", "result = execute_callable(context=context)"),
    ("/home/airflow/.local/lib/python3.12/site-packages/airflow/operators/python.py",
     "execute_callable", "return self.python_callable(*self.op_args, **self.op_kwargs)"),
    ("/opt/airflow/dags/{dag}/{func}.py", "{func}", "{var} = client.fetch({var})"),
    ("/usr/local/lib/python3.11/site-packages/requests/adapters.py", "send",
     "raise ConnectionError(e, request=request)"),
    ("/usr/local/lib/python3.11/site-packages/psycopg2/__init__.py", "connect",
     "conn = _connect(dsn, connection_factory=connection_factory, **kwasync)"),
    ("/usr/local/lib/python3.11/site-packages/botocore/client.py", "_make_api_call",
     "raise error_class(parsed_response, operation_name)"),
    ("/usr/local/lib/python3.11/site-packages/pandas/io/parsers/readers.py", "read_csv",
     "return _read(filepath_or_buffer, kwds)"),
]
NOISE = [
    "{logging_mixin.py:190} INFO - Fetched {n} rows from {schema}.{table}",
    "{base.py:84} INFO - Using connection ID '{user}_conn' for task execution.",
    "{sql.py:{small}} INFO - Running statement: INSERT INTO {schema}.{table} SELECT * FROM tmp",
    "{logging_mixin.py:190} INFO - chunk {small}/{small} uploaded to s3://{bucket}/{date}/",
    "{logging_mixin.py:190} WARNING - Retrying ({small}/5) after transient error",
    "{logging_mixin.py:190} INFO - heartbeat ok",
    "{taskinstance.py:{small}} INFO - ::group::Pre task execution logs",
    "{logging_mixin.py:190} INFO - {func} processed {n} records in {small}.{small}s",
    "{subprocess.py:{small}} INFO - Output: rows_written={n}",
]
# In-house exception classes: the message, not the class, carries the cause.
CUSTOM_CLASSES = [
    "etl.errors.LoadError", "etl.errors.ExtractError", "acme_pipeline.exceptions.TaskFailed",
    "vendor_client.errors.ApiError", "dagutils.StepError", "ingest.core.IngestException",
]
# INFO/WARNING lines that mention other categories' words without being the cause.
DISTRACTORS = [
    "{base.py:84} INFO - Connection to {host}:{port} established",
    "{logging_mixin.py:190} WARNING - Connection reset by peer; retrying (1/5)",
    "{logging_mixin.py:190} INFO - Retry 1/5 succeeded",
    "{logging_mixin.py:190} INFO - Refreshed access token for {user}",
    "{logging_mixin.py:190} INFO - Authenticated as {user}",
    "{logging_mixin.py:190} INFO - Memory usage: {gb} GB of 16 GB",
    "{logging_mixin.py:190} INFO - Query timeout set to {secs}s",
    "{logging_mixin.py:190} INFO - Table {schema}.{table} exists; columns: {col}, id",
    "{logging_mixin.py:190} INFO - Checking for s3://{bucket}/{date}/{file}",
    "{logging_mixin.py:190} INFO - Found {small} files for {date}",
    "{logging_mixin.py:190} INFO - 0 duplicate {col} values in {table}",
    "{warnings.py:110} WARNING - FutureWarning: DataFrame.applymap has been deprecated",
    "{warnings.py:110} WARNING - DeprecationWarning: datetime.utcnow() is deprecated",
    "{logging_mixin.py:190} INFO - Disk usage of /tmp: {small}%",
]

HEADER = [
    "{taskinstance.py:2867} INFO - Starting attempt {small} of {small}",
    "{taskinstance.py:2890} INFO - Executing <Task(PythonOperator): {task}> on {date} "
    "00:00:00+00:00",
    "{standard_task_runner.py:72} INFO - Started process {pid} to run task",
]
FOOTER = [
    "{taskinstance.py:1225} INFO - Marking task as FAILED. dag_id={dag}, task_id={task}",
    "{local_task_job_runner.py:266} INFO - Task exited with return code 1",
]


def _stamp(line: str, v3: bool) -> str:
    ts = f"2026-09-{R.randint(1, 28):02d}T{R.randint(0, 23):02d}:{R.randint(0, 59):02d}:" \
         f"{R.randint(0, 59):02d}"
    if v3:
        return f"[{ts.replace('T', ' ')}] " + line.split("} ", 1)[-1]
    return f"[{ts}.{R.randint(100, 999)}+0000] {line}"


def _frames(k: int) -> list[str]:
    out = []
    for path, fn, code in R.sample(FRAMES, k):
        out.append(f'  File "{fill(path)}", line {R.randint(10, 3000)}, in {fill(fn)}')
        out.append(f"    {fill(code)}")
        if R.random() < 0.3:
            out.append("    " + "^" * R.randint(8, 40))
    return out


def render_sample(category: str, template: str, style: str) -> str:
    cause = fill(template)
    v3 = R.random() < 0.4
    lines = [_stamp(fill(h), v3) for h in HEADER]
    lines += [_stamp(fill(R.choice(NOISE)), v3) for _ in range(R.randint(1, 8))]
    lines += [_stamp(fill(R.choice(DISTRACTORS)), v3) for _ in range(R.randint(0, 4))]
    if style == "custom":
        first, *rest = cause.split("\n")
        cause = "\n".join([f"{R.choice(CUSTOM_CLASSES)}: {first.split(': ', 1)[-1]}", *rest])
        style = "traceback"
    if style == "buried":
        lines += [_stamp(fill(R.choice(NOISE)), v3) for _ in range(R.randint(20, 80))]
        lines.append(_stamp("{logging_mixin.py:190} ERROR - " + cause.split("\n")[0], v3))
        lines += cause.split("\n")[1:]
        lines += [_stamp(fill(R.choice(NOISE)), v3) for _ in range(R.randint(5, 40))]
    elif style == "plain_error":
        lines.append(_stamp("{logging_mixin.py:190} ERROR - " + cause.split("\n")[0], v3))
        lines += cause.split("\n")[1:]
    else:
        lines.append(_stamp("{taskinstance.py:3313} ERROR - Task failed with exception", v3))
        lines.append("Traceback (most recent call last):")
        lines += _frames(R.randint(1, 4))
        if style == "chained":
            lines += cause.split("\n")
            lines += ["", R.choice(CHAIN), "", "Traceback (most recent call last):"]
            lines += _frames(R.randint(1, 3))
            lines.append(fill(R.choice(GENERIC_OUTER)))
        elif style == "reraise":
            first, *rest = cause.split("\n")
            wrapped = R.choice(REWRAP).replace("{cause}", first.split(": ", 1)[-1])
            lines.append(fill(wrapped))
            lines += rest
        else:
            lines += cause.split("\n")
    lines += [_stamp(fill(f), v3) for f in FOOTER]
    if R.random() < 0.15:
        lines = [f"\x1b[3{R.randint(1, 7)}m{line}\x1b[0m" for line in lines]
    return "\n".join(lines)


STYLES = ["traceback", "traceback", "plain_error", "chained", "reraise", "buried", "custom"]


def generate(n: int, seed: int = 7) -> list[dict[str, object]]:
    R.seed(seed)
    per_cat = n // len(TEMPLATES)
    rows: list[dict[str, object]] = []
    for category, templates in TEMPLATES.items():
        for i in range(per_cat):
            t_idx = i % len(templates)
            style = R.choice(STYLES)
            if category == "UNKNOWN" and style == "reraise":
                style = "traceback"
            rows.append(
                {
                    "id": f"syn-{category}-{i}",
                    "label": category,
                    "template_id": f"{category}:{t_idx}",
                    "style": style,
                    "text": render_sample(category, templates[t_idx], style),
                }
            )
    R.shuffle(rows)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=6300)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--out", default=str(DATA / "synthetic.jsonl"))
    args = parser.parse_args()
    rows = generate(args.n, args.seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    templates = sum(len(t) for t in TEMPLATES.values())
    print(f"wrote {len(rows)} samples from {templates} templates to {out}")


if __name__ == "__main__":
    main()
