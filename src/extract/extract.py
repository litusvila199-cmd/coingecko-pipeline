import requests

from src.utils.logger import get_logger
from src.config.settings import API_URL, CRYPTO_IDS

logger = get_logger(__name__)

def extract_data():
    try:
        response = response = requests.get(
            API_URL,
            params={
                "ids": ",".join(CRYPTO_IDS),
                "vs_currencies": "usd"
            }
        )
        response.raise_for_status()

        data = response.json()

        logger.info("Data extracted successfully")

        return data

    except requests.RequestException as e:
        logger.error(f"Error extracting data: {e}")
        raise

if __name__ == "__main__":
    data = extract_data()
    print(data)