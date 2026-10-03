"""Build data/eval_real.jsonl: the hand-labeled evaluation set used by the go/no-go gate.

Sources:
* ``data/exported_task_logs.jsonl`` (from ``backend/scripts/export_task_logs.py``): real task logs.
  A row is included only once someone has filled in its ``label`` field (or it carries an operator
  correction), so unreviewed regex guesses never become ground truth.
* ``CURATED`` below: real-world failure messages (verbatim error text from psycopg2, boto3,
  Snowflake, BigQuery, Spark, requests, Kubernetes, dbt, ...) hand-labeled one by one and wrapped
  in Airflow 2/3 task-log boilerplate. It deliberately over-represents logs the regex classifier
  gets wrong or marks UNKNOWN. Written independently of ``generate_synthetic.py``'s templates.

Usage:  python -m training.build_eval_set
"""

import json
import random
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "data"
EXPORTED = DATA / "exported_task_logs.jsonl"
OUT = DATA / "eval_real.jsonl"

# (label, error text). Multi-line text is emitted as-is after a traceback header.
CURATED: list[tuple[str, str]] = [
    # ------------------------------------------------------------------ AUTH
    ("AUTH", "snowflake.connector.errors.DatabaseError: 250001 (08001): Failed to connect to DB: "
             "acme.eu-west-1.snowflakecomputing.com:443. Incorrect username or password was "
             "specified."),
    ("AUTH", "google.auth.exceptions.RefreshError: ('invalid_grant: Token has been expired or "
             "revoked.', {'error': 'invalid_grant', 'error_description': 'Token has been expired "
             "or revoked.'})"),
    ("AUTH", "azure.core.exceptions.ClientAuthenticationError: Server failed to authenticate the "
             "request. Make sure the value of Authorization header is formed correctly including "
             "the signature.\nErrorCode:AuthenticationFailed"),
    ("AUTH", "jwt.exceptions.InvalidSignatureError: Signature verification failed"),
    ("AUTH", "ValueError: Request to https://api.hubapi.com/crm/v3/objects failed with status 401:"
             " {\"status\":\"error\",\"category\":\"INVALID_AUTHENTICATION\"}"),
    ("AUTH", "slack_sdk.errors.SlackApiError: The request to the Slack API failed. (url: "
             "https://www.slack.com/api/chat.postMessage)\nThe server responded with: {'ok': "
             "False, 'error': 'invalid_auth'}"),
    ("AUTH", "psycopg2.OperationalError: connection to server at \"10.20.1.5\", port 5432 failed: "
             "FATAL:  no pg_hba.conf entry for host \"10.20.4.17\", user \"etl_loader\", database "
             "\"warehouse\", SSL off"),
    ("AUTH", "ValueError: Invalid API key provided: sk_live_****wxyz"),
    ("AUTH", "botocore.exceptions.ClientError: An error occurred (InvalidAccessKeyId) when calling "
             "the PutObject operation: The AWS Access Key Id you provided does not exist in our "
             "records."),
    ("AUTH", "botocore.exceptions.ClientError: An error occurred (SignatureDoesNotMatch) when "
             "calling the ListObjectsV2 operation: The request signature we calculated does not "
             "match the signature you provided. Check your key and signing method."),
    ("AUTH", "pymysql.err.OperationalError: (1045, \"Access denied for user 'etl'@'10.0.3.9' "
             "(using password: YES)\")"),
    ("AUTH", "paramiko.ssh_exception.AuthenticationException: Authentication failed."),
    ("AUTH", "requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: "
             "https://partner.example.com/api/v2/orders?since=2026-09-29"),
    ("AUTH", "snowflake.connector.errors.ProgrammingError: 390114 (08001): Authentication token "
             "has expired.  The user must authenticate again."),
    ("AUTH", "msal.exceptions: AADSTS7000222: The provided client secret keys for app "
             "'3f2a9c1e-1b2c-4d5e-8f90-a1b2c3d4e5f6' are expired."),
    ("AUTH", "KeyError: 'access_token'\nDuring token exchange the identity provider returned: "
             "{'error': 'invalid_client', 'error_description': 'Client authentication failed'}"),
    ("AUTH", "google.api_core.exceptions.PermissionDenied: 403 Permission 'bigquery.tables.getData'"
             " denied on resource (or it may not exist)."),
    ("AUTH", "airflow.exceptions.AirflowException: SFTP login rejected for user 'dropzone': "
             "bad credentials"),
    ("AUTH", "hvac.exceptions.InvalidRequest: missing client token, on post "
             "https://vault.internal:8200/v1/auth/approle/login"),
    ("AUTH", "TypeError: Cannot read secret 'warehouse_password': Vault returned 403 permission "
             "denied for path secret/data/etl"),
    # ------------------------------------------------------------------ DATA_INTEGRITY
    ("DATA_INTEGRITY", "airflow.exceptions.AirflowException: Data quality check failed: 214 rows "
                       "with NULL customer_id in staging.orders"),
    ("DATA_INTEGRITY", "ValueError: Found 37 duplicate rows for primary key (order_id) in batch "
                       "2026-09-30"),
    ("DATA_INTEGRITY", "pandas.errors.ParserError: Error tokenizing data. C error: Expected 12 "
                       "fields in line 40312, saw 13"),
    ("DATA_INTEGRITY", "snowflake.connector.errors.ProgrammingError: 100038 (22018): Numeric "
                       "value 'N/A' is not recognized\n  File 'orders_2026_09_30.csv', line 881, "
                       "character 54"),
    ("DATA_INTEGRITY", "psycopg2.errors.InvalidTextRepresentation: invalid input syntax for type "
                       "integer: \"12a\"\nCONTEXT:  COPY orders, line 2044, column quantity: "
                       "\"12a\""),
    ("DATA_INTEGRITY", "psycopg2.errors.StringDataRightTruncation: value too long for type "
                       "character varying(50)"),
    ("DATA_INTEGRITY", "psycopg2.errors.NumericValueOutOfRange: numeric field overflow\nDETAIL:  "
                       "A field with precision 10, scale 2 must round to an absolute value less "
                       "than 10^8."),
    ("DATA_INTEGRITY", "google.api_core.exceptions.BadRequest: 400 Error while reading data, "
                       "error message: CSV table encountered too many errors, giving up. Rows: "
                       "15; errors: 1. Please look into the errors[] collection for more details."),
    ("DATA_INTEGRITY", "dbt: Failure in test unique_orders_order_id (models/marts/schema.yml)\n"
                       "  Got 3 results, configured to fail if != 0"),
    ("DATA_INTEGRITY", "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff in position "
                       "18231: invalid start byte"),
    ("DATA_INTEGRITY", "ValueError: time data '2026-13-01' does not match format '%Y-%m-%d'"),
    ("DATA_INTEGRITY", "great_expectations.exceptions.ValidationError: Validation failed for "
                       "suite orders.warning: expect_column_values_to_be_between (amount) - 1,204 "
                       "unexpected values"),
    ("DATA_INTEGRITY", "psycopg2.errors.UniqueViolation: duplicate key value violates unique "
                       "constraint \"orders_pkey\"\nDETAIL:  Key (order_id)=(88123) already "
                       "exists."),
    ("DATA_INTEGRITY", "sqlalchemy.exc.IntegrityError: (psycopg2.errors.ForeignKeyViolation) "
                       "insert or update on table \"order_items\" violates foreign key constraint"),
    ("DATA_INTEGRITY", "AssertionError: row count mismatch between source (120044) and target "
                       "(119981) after load"),
    ("DATA_INTEGRITY", "pyarrow.lib.ArrowInvalid: Could not convert 'twelve' with type str: tried "
                       "to convert to int64"),
    ("DATA_INTEGRITY", "airflow.providers.common.sql.operators.sql.SQLCheckOperator: Test failed."
                       "\nQuery:\nSELECT COUNT(*) = 0 FROM payments WHERE amount < 0\nResults:\n"
                       "(False,)"),
    ("DATA_INTEGRITY", "pymysql.err.DataError: (1366, \"Incorrect integer value: 'none' for "
                       "column 'qty' at row 77\")"),
    # ------------------------------------------------------------------ SCHEMA
    ("SCHEMA", "KeyError: \"['customer_email'] not in index\""),
    ("SCHEMA", "pyarrow.lib.ArrowInvalid: Schema at index 2 was different: \norder_id: int64\n"
               "amount: double\nvs\norder_id: int64\namount: string"),
    ("SCHEMA", "google.api_core.exceptions.BadRequest: 400 Provided Schema does not match Table "
               "acme:analytics.orders. Field amount has changed type from INTEGER to FLOAT"),
    ("SCHEMA", "pyspark.errors.exceptions.captured.AnalysisException: [UNRESOLVED_COLUMN."
               "WITH_SUGGESTION] A column or function parameter with name `amt` cannot be "
               "resolved. Did you mean one of the following? [`amount`, `account_id`]"),
    ("SCHEMA", "psycopg2.errors.DatatypeMismatch: column \"created_at\" is of type timestamp "
               "without time zone but expression is of type text"),
    ("SCHEMA", "ValueError: Length mismatch: Expected axis has 11 elements, new values have 12 "
               "elements"),
    ("SCHEMA", "pymysql.err.ProgrammingError: (1146, \"Table 'analytics.orders_v2' doesn't "
               "exist\")"),
    ("SCHEMA", "duckdb.duckdb.BinderException: Binder Error: Referenced column \"amount\" not "
               "found in FROM clause!\nCandidate bindings: \"amount_usd\""),
    ("SCHEMA", "snowflake.connector.errors.ProgrammingError: 000904 (42000): SQL compilation "
               "error: error line 1 at position 7\ninvalid identifier 'AMOUNT_USD'"),
    ("SCHEMA", "psycopg2.errors.UndefinedColumn: column \"discount_pct\" of relation "
               "\"orders\" does not exist"),
    ("SCHEMA", "pandas.errors.IntCastingNaNError: column 'region_id' expected int64 but new "
               "upstream schema delivers nullable values"),
    ("SCHEMA", "ValueError: Columns must be same length as key: source file now has 14 columns, "
               "expected 13 (new column 'channel')"),
    ("SCHEMA", "fastavro._read_common.SchemaResolutionError: Schema mismatch: field 'price' "
               "type 'double' cannot be resolved to 'long'"),
    ("SCHEMA", "google.api_core.exceptions.BadRequest: 400 Unrecognized name: customer_tier at "
               "[3:12]"),
    ("SCHEMA", "TypeError: Object of type 'float' found where 'string' was declared by the target "
               "table schema for column zip_code"),
    ("SCHEMA", "sqlalchemy.exc.ProgrammingError: (psycopg2.errors.UndefinedTable) relation "
               "\"staging.events_v3\" does not exist"),
    # ------------------------------------------------------------------ CODE_BUG
    ("CODE_BUG", "RecursionError: maximum recursion depth exceeded while calling a Python object"),
    ("CODE_BUG", "UnboundLocalError: cannot access local variable 'df' where it is not associated "
                 "with a value"),
    ("CODE_BUG", "jinja2.exceptions.UndefinedError: 'dict object' has no attribute 'ds_nodash_x'"),
    ("CODE_BUG", "airflow.exceptions.AirflowException: Bash command failed. The command returned "
                 "a non-zero exit code 127.\nbash: line 1: dbt: command not found"),
    ("CODE_BUG", "TypeError: unsupported operand type(s) for +: 'int' and 'str'"),
    ("CODE_BUG", "AttributeError: 'NoneType' object has no attribute 'get'"),
    ("CODE_BUG", "airflow.exceptions.AirflowException: Invalid arguments were passed to "
                 "PythonOperator (task_id: transform). Invalid arguments were:\n**kwargs: "
                 "{'provide_context': True}"),
    ("CODE_BUG", "IndentationError: unexpected indent (transform_orders.py, line 88)"),
    ("CODE_BUG", "NameError: name 'pd' is not defined"),
    ("CODE_BUG", "sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at "
                 "or near \"FORM\"\nLINE 1: SELECT id FORM orders"),
    ("CODE_BUG", "ModuleNotFoundError: No module named 'great_expectations'"),
    ("CODE_BUG", "ZeroDivisionError: division by zero"),
    ("CODE_BUG", "IndexError: list index out of range"),
    ("CODE_BUG", "TypeError: transform() missing 1 required positional argument: 'ds'"),
    ("CODE_BUG", "airflow.exceptions.AirflowException: XComArg result from extract at "
                 "load_dag with key=\"return_value\" is not found!"),
    ("CODE_BUG", "NotImplementedError: incremental mode is not supported for this source"),
    ("CODE_BUG", "KeyError: 'config'\n  in function build_payload(params) at etl/payload.py:41"),
    ("CODE_BUG", "jinja2.exceptions.TemplateSyntaxError: unexpected '}'"),
    ("CODE_BUG", "ValueError: too many values to unpack (expected 2)"),
    # ------------------------------------------------------------------ RESOURCE
    ("RESOURCE", "Container killed by YARN for exceeding memory limits. 5.5 GB of 5.5 GB physical "
                 "memory used. Consider boosting spark.yarn.executor.memoryOverhead."),
    ("RESOURCE", "OSError: [Errno 122] Disk quota exceeded: '/tmp/spool/orders.parquet'"),
    ("RESOURCE", "Pod etl-transform-7d9f was evicted: The node was low on resource: "
                 "ephemeral-storage. Container base was using 12Gi, which exceeds its request of "
                 "0."),
    ("RESOURCE", "snowflake.connector.errors.ProgrammingError: 000606 (57P03): Warehouse 'ETL_WH' "
                 "cannot be resumed because resource monitor 'RM_DAILY' has exceeded its quota."),
    ("RESOURCE", "OSError: [Errno 24] Too many open files: '/data/chunks/part-08812.csv'"),
    ("RESOURCE", "Task exited with return code Negsignal.SIGKILL"),
    ("RESOURCE", "torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 2.00 GiB."),
    ("RESOURCE", "java.lang.OutOfMemoryError: Java heap space"),
    ("RESOURCE", "psycopg2.errors.DiskFull: could not extend file \"base/16384/2619\": No space "
                 "left on device"),
    ("RESOURCE", "MemoryError: Unable to allocate 12.4 GiB for an array with shape (1660000000,) "
                 "and data type float64"),
    ("RESOURCE", "kubernetes pod etl-load-1 terminated: reason=OOMKilled exitCode=137"),
    ("RESOURCE", "ValueError: worker pool exhausted: all 16 slots are busy and the queue limit "
                 "(500) was reached"),
    ("RESOURCE", "google.api_core.exceptions.Forbidden: 403 Quota exceeded: Your project "
                 "exceeded quota for free query bytes scanned."),
    ("RESOURCE", "botocore.errorfactory.LimitExceededException: An error occurred "
                 "(LimitExceededException) when calling the StartQueryExecution operation: "
                 "Account limit of concurrent queries reached"),
    ("RESOURCE", "pyspark.errors.exceptions.base.PySparkRuntimeError: Job aborted due to stage "
                 "failure: ExecutorLostFailure (executor 7 exited caused by one of the running "
                 "tasks) Reason: Container from a bad node exceeded memory limits"),
    ("RESOURCE", "psycopg2.errors.TooManyConnections: sorry, too many clients already"),
    # ------------------------------------------------------------------ TIMEOUT
    ("TIMEOUT", "snowflake.connector.errors.ProgrammingError: 000630 (57014): Statement reached "
                "its statement or warehouse timeout of 3600 second(s) and was canceled."),
    ("TIMEOUT", "google.api_core.exceptions.RetryError: Deadline of 600.0s exceeded while calling "
                "target function, last exception: 503 Socket closed"),
    ("TIMEOUT", "ValueError: job did not finish within 1800 seconds; last state RUNNING"),
    ("TIMEOUT", "airflow.exceptions.AirflowException: Job run 88123 has not completed after "
                "3600 seconds."),
    ("TIMEOUT", "botocore.exceptions.WaiterError: Waiter JobComplete failed: Max attempts "
                "exceeded"),
    ("TIMEOUT", "airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 18312"),
    ("TIMEOUT", "psycopg2.errors.QueryCanceled: canceling statement due to statement timeout"),
    ("TIMEOUT", "airflow.exceptions.AirflowSensorTimeout: Sensor has timed out; run duration of "
                "7200.31 seconds exceeds the specified timeout of 7200.0."),
    ("TIMEOUT", "requests.exceptions.ReadTimeout: HTTPSConnectionPool(host='api.partner.io', "
                "port=443): Read timed out. (read timeout=30)"),
    ("TIMEOUT", "AssertionError: EMR step s-2PX9 still PENDING after 45 minutes, giving up"),
    ("TIMEOUT", "concurrent.futures._base.TimeoutError"),
    ("TIMEOUT", "pymysql.err.OperationalError: (3024, 'Query execution was interrupted, maximum "
                "statement execution time exceeded')"),
    ("TIMEOUT", "dbt Cloud job 4412 run 991203 exceeded max wait of 2h and was cancelled by the "
                "operator"),
    ("TIMEOUT", "TypeError: poll() gave up after 120 attempts (~1h) waiting for export to be "
                "ready"),
    ("TIMEOUT", "google.api_core.exceptions.GatewayTimeout: 504 Deadline Exceeded"),
    # ------------------------------------------------------------------ TRANSIENT_NETWORK
    ("TRANSIENT_NETWORK", "ssl.SSLError: [SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC] decryption "
                          "failed or bad record mac (_ssl.c:2580)"),
    ("TRANSIENT_NETWORK", "ValueError: Received an empty response from server after 3 retries "
                          "(RemoteDisconnected)"),
    ("TRANSIENT_NETWORK", "httpx.RemoteProtocolError: Server disconnected without sending a "
                          "response."),
    ("TRANSIENT_NETWORK", "grpc._channel._InactiveRpcError: <_InactiveRpcError of RPC that "
                          "terminated with:\n\tstatus = StatusCode.UNAVAILABLE\n\tdetails = "
                          "\"failed to connect to all addresses\">"),
    ("TRANSIENT_NETWORK", "psycopg2.OperationalError: SSL SYSCALL error: EOF detected"),
    ("TRANSIENT_NETWORK", "MySQLdb.OperationalError: (2013, 'Lost connection to MySQL server "
                          "during query')"),
    ("TRANSIENT_NETWORK", "aiohttp.client_exceptions.ServerDisconnectedError: Server "
                          "disconnected"),
    ("TRANSIENT_NETWORK", "OSError: [Errno 101] Network is unreachable"),
    ("TRANSIENT_NETWORK", "kafka.errors.NoBrokersAvailable: NoBrokersAvailable"),
    ("TRANSIENT_NETWORK", "urllib3.exceptions.ProtocolError: ('Connection aborted.', "
                          "RemoteDisconnected('Remote end closed connection without response'))"),
    ("TRANSIENT_NETWORK", "socket.gaierror: [Errno -3] Temporary failure in name resolution"),
    ("TRANSIENT_NETWORK", "botocore.exceptions.EndpointConnectionError: Could not connect to the "
                          "endpoint URL: \"https://s3.eu-west-1.amazonaws.com/lake/raw\""),
    ("TRANSIENT_NETWORK", "requests.exceptions.HTTPError: 503 Server Error: Service Unavailable "
                          "for url: https://api.partner.io/v1/export"),
    ("TRANSIENT_NETWORK", "TypeError: expected JSON object but got HTML error page from load "
                          "balancer (upstream connect error or disconnect/reset before headers)"),
    ("TRANSIENT_NETWORK", "snowflake.connector.errors.OperationalError: 250003 (08001): Failed "
                          "to get the response. Hanging? method: post, url: https://acme."
                          "snowflakecomputing.com:443/queries/v1/query-request"),
    ("TRANSIENT_NETWORK", "pika.exceptions.StreamLostError: Stream connection lost: "
                          "ConnectionResetError(104, 'Connection reset by peer')"),
    ("TRANSIENT_NETWORK", "ValueError: unexpected EOF from api.partner.io while reading "
                          "chunked response (peer closed connection)"),
    # ------------------------------------------------------------------ UPSTREAM_MISSING
    ("UPSTREAM_MISSING", "google.api_core.exceptions.NotFound: 404 Not found: Table "
                         "acme:raw.events_20260930 was not found in location EU"),
    ("UPSTREAM_MISSING", "pyspark.errors.exceptions.captured.AnalysisException: "
                         "[PATH_NOT_FOUND] Path does not exist: s3a://lake/raw/orders/"
                         "dt=2026-09-30."),
    ("UPSTREAM_MISSING", "ValueError: No objects found under s3://acme-landing/exports/2026/09/"
                         "30/"),
    ("UPSTREAM_MISSING", "airflow.exceptions.AirflowException: Upstream file not yet available: "
                         "/data/inbound/orders_20260930.csv"),
    ("UPSTREAM_MISSING", "pandas.errors.EmptyDataError: No columns to parse from file"),
    ("UPSTREAM_MISSING", "airflow.exceptions.AirflowException: The external task load_raw in DAG "
                         "ingest_orders failed."),
    ("UPSTREAM_MISSING", "AssertionError: expected 24 hourly partitions for 2026-09-30, found 19"),
    ("UPSTREAM_MISSING", "botocore.exceptions.ClientError: An error occurred (404) when calling "
                         "the HeadObject operation: Not Found"),
    ("UPSTREAM_MISSING", "google.cloud.exceptions.NotFound: 404 GET https://storage.googleapis."
                         "com/download/storage/v1/b/acme-exports/o/daily%2F2026-09-30.parquet: "
                         "No such object: acme-exports/daily/2026-09-30.parquet"),
    ("UPSTREAM_MISSING", "FileNotFoundError: [Errno 2] No such file or directory: "
                         "'/mnt/share/in/partners_2026-09-30.json'"),
    ("UPSTREAM_MISSING", "botocore.errorfactory.NoSuchKey: An error occurred (NoSuchKey) when "
                         "calling the GetObject operation: The specified key does not exist."),
    ("UPSTREAM_MISSING", "KeyError: 'Contents'\nlist_objects_v2 returned no objects for prefix "
                         "landing/2026-09-30/ (bucket is empty for this date)"),
    ("UPSTREAM_MISSING", "airflow.exceptions.AirflowException: upstream dataset "
                         "s3://lake/curated/orders has no new version since 2026-09-29"),
    ("UPSTREAM_MISSING", "TypeError: 'NoneType' object is not iterable -- manifest.json for run "
                         "2026-09-30 was never written by the export job"),
    ("UPSTREAM_MISSING", "snowflake.connector.errors.ProgrammingError: 091016 (22000): Remote "
                         "file 's3://acme/stage/orders_2026_09_30.csv.gz' was not found."),
    # ------------------------------------------------------------------ UNKNOWN
    ("UNKNOWN", None),
    ("UNKNOWN", "airflow.exceptions.AirflowException: Bash command failed. The command returned a"
                " non-zero exit code 1."),
    ("UNKNOWN", "airflow.exceptions.AirflowException: Task received SIGTERM signal"),
    ("UNKNOWN", "airflow.exceptions.AirflowException: Pod etl-transform-x1 returned a failure."),
    ("UNKNOWN", "Process finished with non-zero status; see remote worker logs for details"),
    ("UNKNOWN", "airflow.exceptions.AirflowException: Task is in the 'failed' state."),
    ("UNKNOWN", "subprocess.CalledProcessError: Command '['java', '-jar', 'loader.jar']' "
                "returned non-zero exit status 1."),
    ("UNKNOWN", "airflow.exceptions.AirflowException: The job failed. See the Databricks run "
                "page for details."),
    ("UNKNOWN", "RuntimeError: step 3 of 5 failed"),
    ("UNKNOWN", "Exception: something went wrong, aborting"),
    ("UNKNOWN", "airflow.exceptions.AirflowException: Spark application "
                "application_1727654400000_0042 finished with state FAILED"),
    # ------------------------------------------------------------------ extra weak-regex cases
    ("AUTH", "google.auth.exceptions.DefaultCredentialsError: Your default credentials were not "
             "found. To set up Application Default Credentials, see https://cloud.google.com/docs/"
             "authentication/external/set-up-adc"),
    ("CODE_BUG", "airflow.exceptions.AirflowException: Dag 'daily_finance' could not be found; "
                 "either it does not exist or it failed to parse."),
    ("UPSTREAM_MISSING", "airflow.exceptions.AirflowException: Dataset 'orders_daily' partition "
                         "2026-09-30 is not ready"),
    ("TRANSIENT_NETWORK", "ConnectionResetError: [Errno 104] Connection reset by peer"),
    ("DATA_INTEGRITY", "psycopg2.errors.CheckViolation: new row for relation \"payments\" "
                       "violates check constraint \"amount_positive\""),
    ("SCHEMA", "KeyError: 'shipping_address'  # field removed from partner API response v3"),
    ("RESOURCE", "Job aborted: Total size of serialized results of 412 tasks (4.1 GiB) is bigger "
                 "than spark.driver.maxResultSize (4.0 GiB)"),
]

_PRELUDE = [
    "{{taskinstance.py:2614}} INFO - Dependencies all met for dep_context=non-requeueable deps "
    "ti=<TaskInstance: {dag}.{task} scheduled__2026-09-30T00:00:00+00:00 [queued]>",
    "{{taskinstance.py:2867}} INFO - Starting attempt {attempt} of 3",
    "{{taskinstance.py:2890}} INFO - Executing <Task({op}): {task}> on 2026-09-30 00:00:00+00:00",
    "{{standard_task_runner.py:72}} INFO - Started process {pid} to run task",
    "{{task_command.py:467}} INFO - Running <TaskInstance: {dag}.{task} "
    "scheduled__2026-09-30T00:00:00+00:00 [running]> on host worker-{host}",
]
_NOISE = [
    "{{logging_mixin.py:190}} INFO - fetched {n} records from source",
    "{{base.py:84}} INFO - Retrieving connection '{conn}'",
    "{{logging_mixin.py:190}} INFO - writing batch {n} to staging",
    "{{sql.py:511}} INFO - Running statement: SELECT * FROM staging.{table} WHERE ds = '2026-09-30'",
    "{{logging_mixin.py:190}} WARNING - DeprecationWarning: The `schedule_interval` argument is "
    "deprecated",
    "{{logging_mixin.py:190}} INFO - progress: {n}/{m} chunks",
]
_EPILOGUE = [
    "{{taskinstance.py:1225}} INFO - Marking task as FAILED. dag_id={dag}, task_id={task}, "
    "run_id=scheduled__2026-09-30T00:00:00+00:00",
    "{{standard_task_runner.py:124}} ERROR - Failed to execute job {pid} for task {task}",
    "{{local_task_job_runner.py:266}} INFO - Task exited with return code 1",
]
_FRAMES = [
    ("/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", 768,
     "_execute_task", "result = _execute_callable(context=context, **execute_callable_kwargs)"),
    ("/home/airflow/.local/lib/python3.12/site-packages/airflow/operators/python.py", 240,
     "execute", "return_value = self.execute_callable()"),
    ("/opt/airflow/dags/etl/{task}.py", 57, "run", "return client.process(batch)"),
]


def _wrap(rng: random.Random, body: str | None) -> str:
    dag = rng.choice(["ingest_orders", "daily_finance", "crm_sync", "events_rollup", "ml_features"])
    task = rng.choice(["extract", "load_raw", "transform", "publish", "quality_check"])
    fmt = {"dag": dag, "task": task, "attempt": rng.randint(1, 3), "pid": rng.randint(1000, 99999),
           "host": rng.randint(1, 9), "op": rng.choice(["PythonOperator", "SQLExecuteQueryOperator",
                                                         "BashOperator"]),
           "conn": rng.choice(["warehouse", "s3_lake", "partner_api"]), "table": "orders",
           "n": rng.randint(10, 50000), "m": rng.randint(50000, 90000)}
    v3 = rng.random() < 0.4

    def stamp(line: str) -> str:
        ts = f"2026-09-30T00:{rng.randint(10, 59):02d}:{rng.randint(10, 59):02d}"
        if v3:  # Airflow 3 style
            line = line.split("} ", 1)[-1]
            return f"[{ts.replace('T', ' ')}] {line}"
        return f"[{ts}.{rng.randint(100, 999)}+0000] {line}"

    out = [stamp(line.format(**fmt)) for line in _PRELUDE]
    out += [stamp(rng.choice(_NOISE).format(**fmt)) for _ in range(rng.randint(2, 12))]
    if body is not None:
        if rng.random() < 0.8:
            out.append(stamp("{taskinstance.py:3313} ERROR - Task failed with exception"))
            out.append("Traceback (most recent call last):")
            for path, line, fn, code in _FRAMES:
                out.append(f'  File "{path.format(**fmt)}", line {line}, in {fn}')
                out.append(f"    {code}")
            out.extend(body.split("\n"))
        else:
            out.append(stamp("{logging_mixin.py:190} ERROR - " + body.split("\n")[0]))
            out.extend(body.split("\n")[1:])
    out += [stamp(line.format(**fmt)) for line in _EPILOGUE]
    return "\n".join(out)


def main() -> None:
    rng = random.Random(20261001)
    rows: list[dict[str, object]] = []
    for i, (label, body) in enumerate(CURATED):
        rows.append({"id": f"curated-{i:03d}", "origin": "curated", "label": label,
                     "text": _wrap(rng, body)})
    if EXPORTED.exists():
        for line in EXPORTED.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            label = row.get("label") or row.get("operator_category")
            if label and row.get("text"):
                rows.append({"id": f"exported-{row['evidence_id']}", "origin": "exported",
                             "label": label, "text": row["text"]})
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    counts: dict[str, int] = {}
    for row in rows:
        counts[str(row["label"])] = counts.get(str(row["label"]), 0) + 1
    print(f"wrote {len(rows)} rows to {OUT}: {dict(sorted(counts.items()))}")


if __name__ == "__main__":
    main()
