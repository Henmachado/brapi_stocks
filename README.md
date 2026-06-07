# Ingest api data:
```
uv run main.py
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
