import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

from src.utils.logger import get_logger


logger = get_logger(__name__)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


try:
    logger.info("Iniciando proceso de transformación")

    logger.info("Leyendo datos RAW desde S3")

    df = (
        spark.read
        .option("multiline", "true")
        .option("recursiveFileLookup", "true")
        .json("s3://carles-data-lake/coingecko/raw/")
    )

    logger.info("Datos RAW leídos correctamente")

    df = df.select(
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "market_cap_rank",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "circulating_supply",
        "total_supply",
        "last_updated"
    )

    logger.info("Columnas seleccionadas correctamente")

    df = (
        df
        .withColumn("current_price", col("current_price").cast("double"))
        .withColumn("market_cap", col("market_cap").cast("double"))
        .withColumn("market_cap_rank", col("market_cap_rank").cast("integer"))
        .withColumn("total_volume", col("total_volume").cast("double"))
        .withColumn("high_24h", col("high_24h").cast("double"))
        .withColumn("low_24h", col("low_24h").cast("double"))
        .withColumn("price_change_24h", col("price_change_24h").cast("double"))
        .withColumn(
            "price_change_percentage_24h",
            col("price_change_percentage_24h").cast("double")
        )
        .withColumn(
            "circulating_supply",
            col("circulating_supply").cast("double")
        )
        .withColumn(
            "total_supply",
            col("total_supply").cast("double")
        )
    )

    logger.info("Tipos de datos transformados correctamente")

    df = df.withColumn(
        "previous_price_24h",
        col("current_price") - col("price_change_24h")
    )

    logger.info("Columna previous_price_24h creada correctamente")

    df.write.mode("overwrite").parquet(
        "s3://carles-data-lake/coingecko/curated/"
    )

    logger.info("Datos CURATED guardados correctamente en S3")

    job.commit()

    logger.info("Proceso de transformación completado correctamente")

except Exception as e:
    logger.error(
        f"Error en el proceso de transformación: {e}"
    )
    raise