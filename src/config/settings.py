import os 

from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
API_URL = os.getenv("API_URL")
CRYPTO_IDS = os.getenv("CRYPTO_IDS").split(",")