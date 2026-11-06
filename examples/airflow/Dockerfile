FROM astrocrpublic.azurecr.io/runtime:3.2-13

# Resolve the Delta Lake jars once, at build time. Spark tasks that start together would otherwise
# download them into the same Ivy cache and fail on each other's partial files.
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"
