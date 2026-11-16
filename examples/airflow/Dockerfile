FROM astrocrpublic.azurecr.io/runtime:3.2-13

# Development hook: wheels built from this checkout (`uv build --all-packages --out-dir
# examples/airflow/include/wheels`) replace the PyPI install from requirements.txt. A no-op when
# the folder has no wheels, so PyPI stays the default.
RUN if ls include/wheels/*.whl >/dev/null 2>&1; then pip install --no-cache-dir include/wheels/*.whl; fi

# Resolve the Delta Lake jars once, at build time. Spark tasks that start together would otherwise
# download them into the same Ivy cache and fail on each other's partial files.
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"
