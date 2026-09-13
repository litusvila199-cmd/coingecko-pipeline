import pytest
import requests

from unittest.mock import patch, Mock

from src.extract.extract import extract_data, save_to_s3

@patch("src.extract.extract.requests.get")
def test_extract_data_success(mock_get):
    mock_response = Mock()

    mock_response.json.return_value = {
        "bitcoin": {"usd": 78000},
        "ethereum": {"usd": 2500}
    }

    mock_response.raise_for_status.return_value = None

    mock_get.return_value = mock_response

    result = extract_data()

    assert result == {
        "bitcoin": {"usd": 78000},
        "ethereum": {"usd": 2500}
    }

@patch("src.extract.extract.requests.get")
def test_extract_data_request_error(mock_get):
    mock_get.side_effect = requests.RequestException("Connection error")

    with pytest.raises(requests.RequestException):
        extract_data()

@patch("src.extract.extract.s3.put_object")
def tests_save_to_s3_success(mock_put_object):
    data = {
        "bitcoin": {"usd": 78000},
        "ethereum": {"usd": 2500}
    }

    save_to_s3(data)

    mock_put_object.assert_called_once()    