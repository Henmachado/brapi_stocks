# Build using `uv`:
```
uv sync

uv pip install -e .
```

# Ingest api data:
```
uv run main.py -free            # Ingest only free api tickers

uv run main.py -full-api        # Ingest all data from all modules (Token required)
```

# Setup local data lake:
```
uv run jupyter lab --notebook-dir=.
```

Using the `data_lake.ipynb` you can access the tables via PySpark or SQL:

```python
spark.table("activetickers").show()
```

```python
spark.sql("SELECT * FROM activetickers").show()
```
