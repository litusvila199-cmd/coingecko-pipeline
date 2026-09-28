import pandas as pd

from unittest.mock import MagicMock, patch

from src.load.load import load_to_postgres


def test_load_to_postgres():

    df = pd.DataFrame([
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 85806.0,
            "market_cap": 1700000000000.0,
            "market_cap_rank": 1,
            "total_volume": 30000000000.0,
            "high_24h": 86000.0,
            "low_24h": 83000.0,
            "price_change_24h": 1000.0,
            "price_change_percentage_24h": 1.2,
            "circulating_supply": 19000000.0,
            "total_supply": 21000000.0,
            "last_updated": "2026-09-28T16:00:00.000Z",
            "previous_price_24h": 84806.0,
        }
    ])

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor

    with patch(
        "src.load.load.psycopg2.connect",
        return_value=mock_connection
    ):
        load_to_postgres(df)

    mock_connection.commit.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_called_once()