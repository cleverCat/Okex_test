from hyperquant.api import Platform
from hyperquant.clients import Error, ErrorCode
from hyperquant.clients.okex import OkexRESTClient, OkexRESTConverterV1, OkexWSClient, OkexWSConverterV1
from hyperquant.clients.tests.test_init import TestRESTClient, TestWSClient, TestConverter, TestRESTClientHistory

# REST


class TestOkexRESTConverterV1(TestConverter):
    converter_class = OkexRESTConverterV1


class TestOkexRESTClientV1(TestRESTClient):
    platform_id = Platform.OKEX
    # version = "1"


class TestOkexRESTClientHistoryV1(TestRESTClientHistory):
    platform_id = Platform.OKEX
    # version = "1"

    is_to_item_by_id = True

    def test_fetch_trades_history_errors(self):
        super().test_fetch_trades_history_errors()

        # Testing create_rest_client() which must set api_key for Okex
        result = self.client.fetch_trades_history(self.testing_symbol)

        self.assertIsNotNone(result)
        self.assertGoodResult(result)

        # Note: for Okex to get trades history you must send api_key
        self.client.set_credentials(None, None)
        result = self.client.fetch_trades_history(self.testing_symbol)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, Error)
        self.assertEqual(result.code, ErrorCode.UNAUTHORIZED)


# WebSocket


class TestOkexWSConverterV1(TestConverter):
    converter_class = OkexWSConverterV1


class TestOkexWSClientV1(TestWSClient):
    platform_id = Platform.OKEX
    # version = "1"
