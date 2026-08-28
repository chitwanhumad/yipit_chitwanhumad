# yipit_chitwanhumad

Interview assignment repo. Local PySpark + DuckDB project on Python 3.13.

## Prerequisites

- Python 3.13 (`/opt/homebrew/bin/python3.13`)
- Java 17 (`JAVA_HOME` should point at OpenJDK 17)

This machine also has pyenv Python 3.11 on `PATH`. Always use the project `venv` so Spark workers stay on 3.13.

## Setup so far

1. Project folders: `document/`, `config/`, `dbscript/`, `sample_data/`, `src/`
2. Virtualenv created with local Python 3.13
3. Dependencies installed: PySpark 4.2, pandas, pyarrow, DuckDB
4. Spark session helper in `src/spark.py`
5. Paths configured in `config/config.ini` (`PROJECT_ROOT` = `/Users/chitwan.humad/yipit`)

### Recreate the environment

```bash
/opt/homebrew/bin/python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

If `venv` already exists:

```bash
source venv/bin/activate
pip install -r requirements.txt
```

## Config

`config/config.ini` holds Spark settings and file paths.

| Key | Meaning |
| --- | --- |
| `[spark]` | App name, master (`local[*]`), log level, warehouse dir |
| `[paths] PROJECT_ROOT` | Data root: `/Users/chitwan.humad/yipit` |
| `[paths] source_data` | Input: `data/in` under `PROJECT_ROOT` |
| `[paths] output_data` | Output: `data/out` under `PROJECT_ROOT` |
| `[paths] duckdb_path` | DuckDB file: `/Users/chitwan.humad/yipit/warehouse.duckdb` |

Jobs should resolve paths with `get_path()` so they read/write under `PROJECT_ROOT`, not this git repo:

```python
from src.spark import get_duckdb_connection, get_path, get_spark_session

spark = get_spark_session()
conn = get_duckdb_connection()
input_dir = get_path("source_data")   # /Users/chitwan.humad/yipit/data/in
output_dir = get_path("output_data")  # /Users/chitwan.humad/yipit/data/out
db_file = get_path("duckdb_path")     # /Users/chitwan.humad/yipit/warehouse.duckdb
```

`get_duckdb_connection()` opens `duckdb_path` and applies `dbscript/alltables.sql` (creates `dim_currency_conversion` if needed). Do not use bare `duckdb` in the CLI with no file — that is a separate in-memory database and the table will not exist in Python.

Inspect the same file the app uses:

```bash
duckdb /Users/chitwan.humad/yipit/warehouse.duckdb
```

```sql
SELECT * FROM dim_currency_conversion;
```

`src/spark.py` also pins Spark workers to the venv Python so they do not pick up pyenv 3.11.

## Command to run the code

From the project root, with `venv` activated:

```bash
source venv/bin/activate
python -m src.main
```

This runs `src/main.py`, starts Spark, opens DuckDB at `duckdb_path`, and runs the pipeline.

## Spark UI

The Spark UI is at [http://localhost:4040](http://localhost:4040) **only while the Spark session is running**. `src/main.py` calls `spark.stop()` when the pipeline finishes, so the UI is usually already gone by the time you open the browser. There is no history server configured, so you cannot reopen the UI after the process exits.

To inspect the UI, pause before stop (for example `input("Press Enter to stop Spark")`) or print `spark.sparkContext.uiWebUrl` while the session is still open. If 4040 is in use, Spark binds 4041, then 4042, and so on.

## Layout

| Path | Purpose |
| --- | --- |
| `src/` | Jobs (`main.py`) and Spark/path helpers (`spark.py`) |
| `config/config.ini` | Spark, `PROJECT_ROOT`, and `duckdb_path` |
| `dbscript/` | SQL / DB scripts (`alltables.sql` applied on DuckDB connect) |
| `sample_data/` | Repo sample files |
| `document/` | Notes and docs |
| `venv/` | Python 3.13 virtualenv (gitignored) |
| `/Users/chitwan.humad/yipit/data/in` | Source data |
| `/Users/chitwan.humad/yipit/data/out` | Job output |
| `/Users/chitwan.humad/yipit/warehouse.duckdb` | DuckDB database file (`duckdb_path`) |

## Dependencies

See `requirements.txt`:

- `pyspark` (4.x)
- `pandas` (`>=2.2,<3.0`)
- `pyarrow`
- `duckdb`
