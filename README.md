# yipit_chitwanhumad

Interview assignment: a local data pipeline using **Python 3.13**, **PySpark**, and **DuckDB**.

## What you need

- Python 3.13 (use the project `venv`, not pyenv’s Python 3.11)
- Java 17 (required by Spark)

## What matters

| What | Where |
| --- | --- |
| Settings | `config/config.ini` |
| Data files | `/Users/chitwan.humad/yipit/data/in` and `data/out` |
| DuckDB database | `/Users/chitwan.humad/yipit/warehouse.duckdb` |
| Table scripts | `dbscript/alltables.sql` |
| Pipeline entry | `src/main.py` |

`config.ini` points Spark and DuckDB at those paths. The app opens the warehouse file and creates tables if they are missing. Currency rates are seeded once and are not inserted again on later runs.

## Spark UI

The Spark UI at http://localhost:4040 is only available **while the job is running**. The session is closed when the pipeline finishes, so the UI is usually already gone.

## Folders

- `src/` — pipeline code (`main.py`, Spark/DuckDB helpers, currency conversion)
- `config/` — settings
- `dbscript/` — SQL
- `document/` and `sample_data/` — notes and samples
- `venv/` — local Python environment (not in git)

### If any issue with pyspark module installation on windows

# Activate venv
.\venv\Scripts\Activate.ps1

# Then upgrade setuptools
python -m pip install --upgrade pip setuptools wheel

# Install requirements
pip install -r requirements.txt