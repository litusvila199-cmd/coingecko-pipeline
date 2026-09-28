import boto3
import pandas as pd
import psycopg2

from io import BytesIO

from src.config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    S3_BUCKET,
)
from src.utils.logger import get_logger


logger = get_logger(__name__)

s3 = boto3.client("s3")


def read_from_s3():
    """Lee el archivo Parquet CURATED desde S3."""

    objects = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix="coingecko/curated/"
    )

    parquet_file = next(
        obj["Key"]
        for obj in objects["Contents"]
        if obj["Key"].endswith(".parquet")
    )

    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=parquet_file
    )

    df = pd.read_parquet(
        BytesIO(response["Body"].read())
    )

    logger.info("Datos CURATED leídos correctamente")
    logger.info(f"Registros leídos: {len(df)}")

    return df


def load_to_postgres(df):
    """Carga los datos del DataFrame en PostgreSQL."""

    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    cursor = connection.cursor()

    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO crypto_prices (
                id,
                symbol,
                name,
                current_price,
                market_cap,
                market_cap_rank,
                total_volume,
                high_24h,
                low_24h,
                price_change_24h,
                price_change_percentage_24h,
                circulating_supply,
                total_supply,
                last_updated,
                previous_price_24h
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            );
            """,
            (
                row["id"],
                row["symbol"],
                row["name"],
                row["current_price"],
                row["market_cap"],
                row["market_cap_rank"],
                row["total_volume"],
                row["high_24h"],
                row["low_24h"],
                row["price_change_24h"],
                row["price_change_percentage_24h"],
                row["circulating_supply"],
                row["total_supply"],
                row["last_updated"],
                row["previous_price_24h"],
            )
        )

    connection.commit()

    cursor.close()
    connection.close()

    logger.info("Datos cargados en PostgreSQL correctamente")
    logger.info("Conexión a PostgreSQL cerrada")


def main():
    """Ejecuta el proceso completo de carga."""

    try:
        df = read_from_s3()
        load_to_postgres(df)

    except psycopg2.Error as e:
        logger.error(f"Error en PostgreSQL: {e}")
        raise

    except Exception as e:
        logger.error(f"Error en el proceso de carga: {e}")
        raise


if __name__ == "__main__":
    main()