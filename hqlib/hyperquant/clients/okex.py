import hashlib
import zlib
import re
import hmac
import time
from threading import Thread
from operator import itemgetter

from hyperquant.api import Platform, Sorting, Interval, Direction, OrderType
from hyperquant.clients import WSClient, Endpoint, Trade, Error, ErrorCode, \
    ParamName, WSConverter, RESTConverter, PrivatePlatformRESTClient,\
    MyTrade, Candle, Ticker, OrderBookItem, Order, \
    OrderBook, Account, Balance
from websocket import WebSocketApp

# REST


# TODO check getting trades history from_id=1
class OkexRESTConverterV1(RESTConverter):
    # Main params:
    base_url = "https://www.okex.com/api/v{version}/"

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE: "trades.do",
        Endpoint.TRADE_HISTORY: "trades.do",
        Endpoint.CANDLE: "kline.do",
    }
    param_name_lookup = {
        ParamName.SYMBOL: "symbol",
        ParamName.LIMIT: "size",
        ParamName.IS_USE_MAX_LIMIT: None,
        # ParamName.SORTING: None,
        ParamName.INTERVAL: "type",
        ParamName.DIRECTION: "side",
        ParamName.ORDER_TYPE: "type",
        ParamName.TIMESTAMP: "timestamp",
        ParamName.FROM_ITEM: "fromId",
        ParamName.TO_ITEM: None,
        ParamName.FROM_TIME: "startTime",
        ParamName.TO_TIME: "endTime",
        ParamName.PRICE: "price",
        ParamName.AMOUNT: "quantity",
        # -ParamName.ASKS: "asks",
        # ParamName.BIDS: "bids",
    }
    param_value_lookup = {
        Sorting.DEFAULT_SORTING: Sorting.ASCENDING,
        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5min",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "1day",
        Interval.WEEK_1: "1week",

        # By properties:
        ParamName.DIRECTION: {
            Direction.SELL: "SELL",
            Direction.BUY: "BUY",
        },
        ParamName.ORDER_TYPE: {
            OrderType.LIMIT: "LIMIT",
            OrderType.MARKET: "MARKET",
        },
        # ParamName.ORDER_STATUS: {
        #     OrderStatus.: "",
        # },
    }
    max_limit_by_endpoint = {
        Endpoint.TRADE: 1000,
        Endpoint.TRADE_HISTORY: 1000,
        Endpoint.ORDER_BOOK: 1000,
        Endpoint.CANDLE: 1000,
    }

    # For parsing

    param_lookup_by_class = {
        # Error
        Error: {
            "code": "code",
            "msg": "message",
        },
        # Data
        Trade: {
            "date_ms": ParamName.TIMESTAMP,
            "tid": ParamName.ITEM_ID,
            "price": ParamName.PRICE,
            "amount": ParamName.AMOUNT,
            "type": ParamName.DIRECTION,
            # "isBuyerMaker": "",
            # "isBestMatch": "",
        },
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT,  # only volume present
            #None,
            #None,
            #ParamName.TRADES_COUNT,
            # ParamName.INTERVAL,
        ],
    }

    error_code_by_platform_error_code = {
        1024: ErrorCode.WRONG_SYMBOL,
        1008: ErrorCode.WRONG_PARAM,
    }
    error_code_by_http_status = {
        429: ErrorCode.RATE_LIMIT,
        418: ErrorCode.IP_BAN,
    }

    # For converting time
    is_source_in_milliseconds = True


    def _process_param_value(self, name, value):
        if name == ParamName.FROM_ITEM or name == ParamName.TO_ITEM:
            if isinstance(value, Trade):  # ItemObject):
                return value.item_id
        return super()._process_param_value(name, value)

    def parse(self, endpoint, data):
        if endpoint == Endpoint.SERVER_TIME and data:
            timestamp_ms = data.get("serverTime")
            return timestamp_ms / 1000 if not self.use_milliseconds and timestamp_ms else timestamp_ms
        if endpoint == Endpoint.SYMBOLS and data and ParamName.SYMBOLS in data:
            exchange_info = data[ParamName.SYMBOLS]
            # (There are only 2 statuses: "TRADING" and "BREAK")
            # symbols = [item[ParamName.SYMBOL] for item in exchange_info if item["status"] == "TRADING"]
            symbols = [item[ParamName.SYMBOL] for item in exchange_info]
            return symbols

        result = super().parse(endpoint, data)
        return result


class OkexRESTClient(PrivatePlatformRESTClient):
    # Settings:
    platform_id = Platform.OKEX
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": OkexRESTConverterV1,
        "3":
        OkexRESTConverterV1,  # Only for some methods (same converter used)
    }

    # State:
    ratelimit_error_in_row_count = 0

    @property
    def headers(self):
        result = super().headers
        result["X-MBX-APIKEY"] = self._api_key
        result["Content-Type"] = "application/x-www-form-urlencoded"
        return result

    def _on_response(self, response, result):
        # super()._on_response(response, result)

        self.delay_before_next_request_sec = 0
        if isinstance(result, Error):
            if result.code == ErrorCode.RATE_LIMIT:
                self.ratelimit_error_in_row_count += 1
                self.delay_before_next_request_sec = 60 * 2 * self.ratelimit_error_in_row_count  # some number - change
            elif result.code == ErrorCode.IP_BAN:
                self.ratelimit_error_in_row_count += 1
                self.delay_before_next_request_sec = 60 * 5 * self.ratelimit_error_in_row_count  # some number - change
            else:
                self.ratelimit_error_in_row_count = 0
        else:
            self.ratelimit_error_in_row_count = 0

    def fetch_history(self,
                      endpoint,
                      symbol,
                      limit=None,
                      from_item=None,
                      to_item=None,
                      sorting=None,
                      is_use_max_limit=False,
                      from_time=None,
                      to_time=None,
                      version=None,
                      **kwargs):
        if from_item is None:
            from_item = 0
        return super().fetch_history(endpoint, symbol, limit, from_item,
                                     to_item, sorting, is_use_max_limit,
                                     from_time, to_time, **kwargs)

    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        if endpoint in self.converter.secured_endpoints:
            server_timestamp = self.get_server_timestamp()
            params[
                ParamName.
                TIMESTAMP] = server_timestamp if self.use_milliseconds else int(
                    server_timestamp * 1000)
        return super()._send(method, endpoint, params, version, **kwargs)


# WebSocket


class OkexWSConverterV1(WSConverter):
    # Main params:
    base_url = "wss://real.okex.com:10440/ws/v{version}"

    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True

    # supported_endpoints = [Endpoint.TRADE]
    # symbol_endpoints = [Endpoint.TRADE]
    # supported_symbols = None

    # Settings:

    # Converting info:
    # For converting to platform

    endpoint_lookup = {
        Endpoint.TRADE: "ok_sub_spot_{symbol}_deals",
        Endpoint.CANDLE: "ok_sub_spot_{symbol}_kline_{interval}",
    }
    param_value_lookup = {
        Sorting.DEFAULT_SORTING: Sorting.ASCENDING,
        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5min",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "1day",
        Interval.WEEK_1: "1week",

        # By properties:
        ParamName.DIRECTION: {
            Direction.SELL: "SELL",
            Direction.BUY: "BUY",
        },
        ParamName.ORDER_TYPE: {
            OrderType.LIMIT: "LIMIT",
            OrderType.MARKET: "MARKET",
        },
        # ParamName.ORDER_STATUS: {
        #     OrderStatus.: "",
        # },
    }

    # For parsing
    param_lookup_by_class = {
        # Error
        Error: {
            # "code": "code",
            # "msg": "message",
        },
        # Data
        Trade: [
            ParamName.ITEM_ID,
            ParamName.PRICE,
            ParamName.AMOUNT,
            ParamName.TIME,
            ParamName.DIRECTION,
            ParamName.SYMBOL,
        ],
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT,  # only volume present
            ParamName.SYMBOL,
        ]
    }
    endpoint_by_event_type = {
        "trade": Endpoint.TRADE,
        "kline": Endpoint.CANDLE,
    }

    # https://github.com/binance-exchange/binance-official-api-docs/blob/master/errors.md
    error_code_by_platform_error_code = {
        1024: ErrorCode.WRONG_SYMBOL,
        2007: ErrorCode.WRONG_PARAM,
    }
    error_code_by_http_status = {}

    # For converting time
    is_source_in_milliseconds = True

    def _generate_subscription(self, endpoint, symbol=None, **params):
        return super()._generate_subscription(
            endpoint,
            symbol.lower() if symbol else symbol, **params), endpoint

    def parse(self, endpoint, data):
        if "data" in data:
            channel = data['channel']
            symbol_regexp = None
            if 'deals' in channel:
                symbol_regexp = re.search('ok_sub_spot_(.+?)_deals', channel)
            if 'kline' in channel:
                symbol_regexp = re.search('ok_sub_spot_(.+?)_kline_*', channel)
            symbol = None
            if symbol_regexp:
                symbol = symbol_regexp.group(1)
                for i in data['data']:
                    i += [symbol]
            data = data["data"]
        return super().parse(endpoint, data)

    def _parse_item(self, endpoint, item_data):
        return super()._parse_item(endpoint, item_data)


class OkexWSClient(WSClient):
    platform_id = Platform.OKEX
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": OkexWSConverterV1,
    }
    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True
    _channel_to_endpoint = {}

    def _subscribe(self, subscriptions):
        self.subscriptions_data = subscriptions

        return super()._subscribe(subscriptions)

    def _on_message(self, message):
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompress.decompress(message)
        inflated += decompress.flush()
        return super()._on_message(inflated.decode('utf-8'))

    def _send_subscribe(self, subscriptions):
        self.logger.debug('_send_subscribe')
        self.logger.debug(subscriptions)
        for channel, endpoint in subscriptions:
            self.logger.debug(channel)
            self._channel_to_endpoint[channel] = endpoint
            event_data = {"event": "addChannel", "channel": channel}
            self._send(event_data)

    def _parse(self, endpoint, data):
        batch_data = []
        for i in data:
            current_endpoint = self._channel_to_endpoint.get(i['channel'])
            batch_data += super()._parse(current_endpoint, i)
        return batch_data

    def connect(self, version=None):
        self.logger.debug("connect")
        # Check ready
        if not self.current_subscriptions:
            self.logger.warning("Please subscribe before connect.")
            return

        # Do nothing if was called before
        if self.ws and self.is_started:
            self.logger.warning("WebSocket is already started.")
            return

        # Connect
        if not self.ws:
            self.ws = WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close)
        else:
            self.ws.url = self.url
            self.ws.header = self.headers

        # (run_forever() will raise an exception if previous socket is still not closed)
        self.logger.debug("Start WebSocket with url: %s" % self.ws.url)
        self.is_started = True

        def send_heart_beat(ws):
            ping = '{"event":"ping"}'
            while (True):
                sent = False
                while (sent is False):
                    try:
                        ws.send(ping)
                        sent = True
                    except Exception as e:
                        raise e
                time.sleep(30)

        self.thread = Thread(target=send_heart_beat, args=(self.ws, ))
        self.ws.run_forever()
        self.thread.daemon = True
        self.thread.start()
