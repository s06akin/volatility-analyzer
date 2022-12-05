import requests
import json

import pandas as pd
from glom import glom
import time

import base64
import hashlib
import hmac
import datetime
from urllib import parse
import urllib.parse



# Params

base_quote = 'btc'
barrier = 0.5
result_trading_pairs = 3

timeframes = {
    '3h_1m': [180, '1min'],
    '6h_1m': [360, '1min'],
    '24h_5m': [288, '5min'],
    '48h_5m': [576, '5min'],
    '72h_15m': [288, '15min']
}

pump_dump_filter = {
    '15min': [3, '15min', 10],
    '60min': [3, '60min', 20],
    '4hour': [3, '4hour', 30],
    '1day': [3, '1day', 50]
}

bots = [[582818, 'nexo/btc', 'working', 'normal'],
        [582817, 'doge/btc', 'waiting', 'filter'],
        [582703, 'uni/btc', 'working', 'normal']]

url = 'https://app.revenuebot.io/external/tv'

newHeaders = {'Content-type': 'application/json', 'Accept': 'application/json'}

data = {
    "action": "enter_cycle",
    "ref_token": "a06e744b-b5e6-11ec-9e28-8e4b67c9fcba",
    "bot_id": 582818,
    "pair": "ada/btc"
}

huobi_api_key = "8ac3009b-9e4c7392-ed2htwf5tf-7e1e3"
huobi_secret_key = "3f2b17c9-5080f327-9460b099-e62fd"

cmc_req_cnt = 0

# Huobi SDK

class TypeCheck:
    @staticmethod
    def is_list(obj):
        return type(obj) == list and isinstance(obj, list)

    @staticmethod
    def is_dict(obj):
        return type(obj) == dict and isinstance(obj, dict)


def get_default_server_url(user_configed_url):
    if user_configed_url and len(user_configed_url):
        return user_configed_url
    else:
        return "https://api.huobi.pro"


class Account:

    def __init__(self):
        self.id = 0
        self.type = None
        self.state = None
        self.subtype = ""


class AccountClient(object):

    def __init__(self, **kwargs):
        self.__kwargs = kwargs

    def get_accounts(self):
        return GetAccountsService({}).request(**self.__kwargs)

    # def get_balance(self, account_id: 'int'):
    #     params = {
    #         "account-id": account_id
    #     }
    #     return GetBalanceService(params).request(**self.__kwargs)

    def get_account_by_type_and_symbol(self, account_type, symbol):
        accounts = self.get_accounts()
        if accounts and len(accounts):
            for account_obj in accounts:
                if account_obj.type == account_type:
                    if account_type == "margin":
                        if symbol == account_obj.subtype:
                            return account_obj
                    else:
                        return account_obj

        return None


class GetAccountsService:

    def __init__(self, params):
        self.params = params

    def request(self, **kwargs):
        channel = "/v1/account/accounts"

        def parse(dict_data):
            data_list = dict_data.get("data", [])
            return default_parse_list_dict(data_list, Account, [])

        return RestApiSyncClient(**kwargs).request_process("GET_SIGN", channel, self.params, parse)


def key_trans(key_origin):
    if key_origin and len(key_origin) > 1:
        return key_origin.replace("-", "_")
    else:
        return ""


def fill_obj(dict_data, class_name=object):
    obj = class_name()
    for ks, vs in dict_data.items():
        obj_key = key_trans(ks)
        if hasattr(obj, obj_key):
            setattr(obj, obj_key, vs)
            continue
    return obj


def fill_obj_list(list_data, class_name):
    if (TypeCheck.is_list(list_data)):
        inner_obj_list = list()
        for idx, row in enumerate(list_data):
            inner_obj = fill_obj(row, class_name)
            inner_obj_list.append(inner_obj)
        return inner_obj_list

    return list()


def default_parse_list_dict(inner_data, inner_class_name=object, default_value=None):
    new_value = default_value
    if inner_data and len(inner_data):
        if (TypeCheck.is_list(inner_data)):
            new_value = fill_obj_list(inner_data, inner_class_name)
        elif (TypeCheck.is_dict(inner_data)):
            new_value = fill_obj(inner_data, inner_class_name)
        else:
            new_value = default_value

    return new_value


class RestApiSyncClient(object):

    def __init__(self, **kwargs):
        self.__api_key = kwargs.get("api_key", None)
        self.__secret_key = kwargs.get("secret_key", None)
        self.__server_url = kwargs.get("url", get_default_server_url(None))
        self.__init_log = kwargs.get("init_log", None)
        self.__performance_test = kwargs.get("performance_test", None)

    def __create_request_by_get_with_signature(self, url, builder):
        request = RestApiRequest()
        request.method = "GET"
        request.host = self.__server_url
        create_signature(self.__api_key, self.__secret_key, request.method, request.host + url, builder)
        request.header.update({"Content-Type": "application/x-www-form-urlencoded"})
        request.url = url + builder.build_url()
        return request

    def create_request(self, method, url, params, parse):
        builder = UrlParamsBuilder()
        if params and len(params):
            if method in ["GET", "GET_SIGN"]:
                for key, value in params.items():
                    builder.put_url(key, value)

        if method == "GET_SIGN":
            request = self.__create_request_by_get_with_signature(url, builder)

        request.json_parser = parse

        return request

    def request_process(self, method, url, params, parse):
        return self.request_process_product(method, url, params, parse)

    def request_process_product(self, method, url, params, parse):
        request = self.create_request(method, url, params, parse)
        if request:
            return call_sync(request)

        return None


class RestApiRequest(object):

    def __init__(self):
        self.method = ""
        self.url = ""
        self.host = ""
        self.post_body = ""
        self.header = dict()
        self.json_parser = None


def create_signature(api_key, secret_key, method, url, builder):
    timestamp = utc_now()
    builder.put_url("AccessKeyId", api_key)
    builder.put_url("SignatureVersion", "2")
    builder.put_url("SignatureMethod", "HmacSHA256")
    builder.put_url("Timestamp", timestamp)

    host = urllib.parse.urlparse(url).hostname
    path = urllib.parse.urlparse(url).path

    keys = sorted(builder.param_map.keys())
    qs0 = '&'.join(['%s=%s' % (key, parse.quote(builder.param_map[key], safe='')) for key in keys])
    payload0 = '%s\n%s\n%s\n%s' % (method, host, path, qs0)
    dig = hmac.new(secret_key.encode('utf-8'), msg=payload0.encode('utf-8'), digestmod=hashlib.sha256).digest()
    s = base64.b64encode(dig).decode()
    builder.put_url("Signature", s)


def utc_now():
    return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')


class UrlParamsBuilder(object):

    def __init__(self):
        self.param_map = dict()
        self.post_map = dict()
        self.post_list = list()

    def put_url(self, name, value):
        if value is not None:
            if isinstance(value, (list, dict)):
                self.param_map[name] = value
            else:
                self.param_map[name] = str(value)

    def put_post(self, name, value):
        if value is not None:
            if isinstance(value, (list, dict)):
                self.post_map[name] = value
            else:
                self.post_map[name] = str(value)

    def build_url(self):
        if len(self.param_map) == 0:
            return ""
        encoded_param = urllib.parse.urlencode(self.param_map)
        return "?" + encoded_param

    def build_url_to_json(self):
        return json.dumps(self.param_map)


session_huobi = requests.Session()


def call_sync(request, is_checked=False):
    if request.method == "GET":
        response = session_huobi.get(request.host + request.url, headers=request.header)
        if is_checked is True:
            return response.text
        dict_data = json.loads(response.text.encode().decode('utf-8-sig'))
        return request.json_parser(dict_data)

    def __create_request_by_get_with_signature(self, url, builder):
        request = RestApiRequest()
        request.method = "GET"
        request.host = self.__server_url
        create_signature(self.__api_key, self.__secret_key, request.method, request.host + url, builder)
        request.header.update({"Content-Type": "application/x-www-form-urlencoded"})
        request.url = url + builder.build_url()
        return request


class TradeClient(object):

    def __init__(self, **kwargs):
        """
        Create the request client instance.
        :param kwargs: The option of request connection.
            api_key: The public key applied from Huobi.
            secret_key: The private key applied from Huobi.
            url: The URL name like "https://api.huobi.pro".
            init_log: to init logger
        """
        self.__kwargs = kwargs

    def get_open_orders(self, symbol: 'str', account_id: 'int', side: 'OrderSide' = None,
                        size: 'int' = None, from_id=None, direct=None) -> list:
        """
        The request of get open orders.

        :param symbol: The symbol, like "btcusdt". (mandatory)
        :param account_id: account id (mandatory)
        :param side: The order side, buy or sell. If no side defined, will return all open orders of the account. (optional)
        :param size: The number of orders to return. Range is [1, 500]. (optional)
        :param direct: 1:prev  order by ID asc from from_id, 2:next order by ID desc from from_id
        :param from_id: start ID for search
        :return: The orders information.
        """
        params = {
            "symbol": symbol,
            "account-id": account_id,
            "side": side,
            "size": size,
            "from": from_id,
            "direct": direct
        }

        return GetOpenOrdersService(params).request(**self.__kwargs)


class GetOpenOrdersService:

    def __init__(self, params):
        self.params = params

    def request(self, **kwargs):
        channel = "/v1/order/openOrders"

        def parse(dict_data):
            data_list = dict_data.get("data", [])
            return Order.json_parse_list(data_list)

        return RestApiSyncClient(**kwargs).request_process("GET_SIGN", channel, self.params, parse)


class Order:

    def __init__(self):
        self.id = 0
        self.symbol = ""
        self.account_id = 0
        self.amount = 0.0
        self.price = 0.0
        self.created_at = 0
        self.canceled_at = 0
        self.finished_at = 0
        self.type = None
        self.filled_amount = 0.0
        self.filled_cash_amount = 0.0
        self.filled_fees = 0.0
        self.source = None
        self.state = None
        self.client_order_id = ""
        self.stop_price = ""
        self.next_time = 0
        self.operator = ""

    @staticmethod
    def json_parse(json_data):
        order = fill_obj(json_data, Order)
        order.filled_amount = json_data.get("filled-amount", json_data.get("field-amount", 0))
        order.filled_cash_amount = json_data.get("filled-cash-amount", json_data.get("field-cash-amount", 0))
        order.filled_fees = json_data.get("filled-fees", json_data.get("field-fees", 0))
        return order

    @staticmethod
    def json_parse_list(json_data):
        if json_data and len(json_data):
            order_list = list()
            for idx, row in enumerate(json_data):
                order_item = Order.json_parse(row)
                order_list.append(order_item)
            return order_list

        return list()

    def print_object(self, format_data=""):
        PrintBasic.print_basic(self.id, format_data + "Order Id")
        PrintBasic.print_basic(self.symbol, format_data + "Symbol")
        PrintBasic.print_basic(self.price, format_data + "Price")
        PrintBasic.print_basic(self.amount, format_data + "Amount")


#         PrintBasic.print_basic(self.created_at, format_data + "Create Time")
#         PrintBasic.print_basic(self.canceled_at, format_data + "Cancel Time")
#         PrintBasic.print_basic(self.finished_at, format_data + "Finish Time")
#         PrintBasic.print_basic(self.type, format_data + "Order Type")
#         PrintBasic.print_basic(self.filled_amount, format_data + "Filled Amount")
#         PrintBasic.print_basic(self.filled_cash_amount, format_data + "Filled Cash Amount")
#         PrintBasic.print_basic(self.filled_fees, format_data + "Filled Fees")
#         PrintBasic.print_basic(self.source, format_data + "Order Source")
#         PrintBasic.print_basic(self.state, format_data + "Order State")
#         PrintBasic.print_basic(self.client_order_id, format_data + "Client Order Id")
#         PrintBasic.print_basic(self.stop_price, format_data + "Stop Price")
#         PrintBasic.print_basic(self.operator, format_data + "Operator")
#         PrintBasic.print_basic(self.next_time, format_data + "Next Time")

# for obj in list_obj:
#     obj.print_object()
#     print()


class PrintBasic:
    @staticmethod
    def print_basic(data, name=None):
        if name and len(name):
            print(str(name) + " : " + str(data))
        else:
            print(str(data))


# MAIN

while True:

    if cmc_req_cnt == 0 or cmc_req_cnt == 12:

        # Coinmarketcap API
        url_cmc = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

        parameters_cmc = {
          'start': '1',
          'limit': '200'
        }

        headers_cmc = {
          'Accepts': 'application/json',
          'X-CMC_PRO_API_KEY': '7326fb8d-6d2a-4017-b422-fbaf8ac4801b'
        }

        session_cmc = requests.Session()
        session_cmc.headers.update(headers_cmc)

        response_cmc = session_cmc.get(url_cmc, params=parameters_cmc).json()
        data_cmc = pd.DataFrame(response_cmc['data'])


        # Exclude stablecoins

        stablecoins_list = []
        for i in data_cmc.index:
            for j in range(len(data_cmc['tags'][i])):
                if data_cmc['tags'][i][j] == 'stablecoin':
                    stablecoins_list.append(i)

        stablecoins = pd.DataFrame(data_cmc.iloc[stablecoins_list])
        data_cmc = data_cmc.loc[~data_cmc['id'].isin(list(stablecoins['id']))]
        data_cmc = data_cmc.reset_index(drop = True)


        # Top 100 coins with Bitcoin Price Equivalence < Bitcoin Price

        df = pd.DataFrame(data_cmc, columns=['id', 'symbol', 'cmc_rank', 'total_supply', 'quote'])

        price_usd = pd.DataFrame(df['quote'].apply(lambda row: glom(row, 'USD.price')))
        price_usd = price_usd.rename(columns={'quote': 'price_usd'})

        df.drop('quote', axis=1, inplace=True)

        df = df.merge(price_usd, left_index=True, right_index=True)

        btc_total_supply = df[df['id'] == 1]['total_supply']
        df = df.assign(BPE = df.total_supply / btc_total_supply[0] * df.price_usd)
        df = df[(df['cmc_rank'] <= 100) & (df['BPE'] <= df[df['id'] == 1]['BPE'][0])]
        df = df.set_index('id')
        df = pd.DataFrame(df['symbol'].str.lower() + base_quote)

        cmc_req_cnt = 0

    # Huobi API

    url_huobi_symbols = 'https://api.huobi.pro/v1/common/symbols'

    # session_huobi = requests.Session()

    repsonse_huobi = session_huobi.get(url_huobi_symbols).json()
    hs = pd.DataFrame(repsonse_huobi['data'], columns=['symbol', 'state'])

    hs = hs[hs['state'] == 'online']
    hs.drop('state', axis=1, inplace=True)
    hs = hs.loc[hs['symbol'].str.endswith(base_quote)]

    rdf = pd.merge(df, hs, on='symbol', how='inner')


    # Pump/Dump filter

    for key, value in pump_dump_filter.items():
        rdf[key] = ''
        size = value[0]
        period = value[1]
        pd_percent = value[2]

        rdf['vol'] = ''

        for i, j in enumerate(rdf['symbol']):
            symbol = rdf['symbol'][i]
            pdf_url = f'https://api.huobi.pro/market/history/kline?symbol={symbol}&period={period}&size={size}'
            repsonse = session_huobi.get(pdf_url).json()
            pdf = pd.DataFrame(repsonse['data'], columns=['open', 'close', 'vol'])
            pdf['%_price_change'] = abs(pdf['open'] - pdf['close']) / pdf['open'] * 100
            rdf.loc[rdf['symbol'] == symbol, [key]] = pdf['%_price_change'].max()
            rdf.loc[rdf['symbol'] == symbol, ['vol']] = pdf['vol'].max()


    rdf[key] = rdf[key].astype(int)
    rdf = rdf[rdf[key] < pd_percent]


    # Multi directional fluctuations

    rdf = pd.merge(df, hs, on='symbol', how='inner')

    for key, value in timeframes.items():
        rdf[key] = ''
        size = value[0]
        period = value[1]

        for i, j in enumerate(rdf['symbol']):
            symbol = rdf['symbol'][i]
            mdf_url = f'https://api.huobi.pro/market/history/kline?symbol={symbol}&period={period}&size={size}'

            repsonse = session_huobi.get(mdf_url).json()
            mf = pd.DataFrame(repsonse['data'], columns=['high', 'low'])
            mf['fluct'] = (mf['high'] / ((mf['high'] - mf['low']) / 2 + mf['low']) - 1) * 100
            mf['count'] = mf['fluct'] > barrier
            mf['count'] = mf['count'].astype(int)
            mf = mf['count'].sum()
            rdf.loc[rdf['symbol'] == symbol, [key]] = mf


    rdf['multi_fluct'] = rdf['3h_1m'] * 1.1 + rdf['6h_1m'] * 1.2 + rdf['24h_5m'] * 1.3 + rdf['48h_5m'] * 1.4 + rdf[
        '72h_15m'] * 1.5
    mf_list = list(rdf.sort_values(by='multi_fluct', ascending=False)['symbol'].head(result_trading_pairs))

    for i, j in enumerate(mf_list):
        mf_list[i] = mf_list[i][:-3] + '/' + mf_list[i][-3:]


    # Huobi open orders

    account_client = AccountClient(api_key=huobi_api_key, secret_key=huobi_secret_key)
    account_spot = account_client.get_account_by_type_and_symbol(account_type="spot", symbol=None)
    account_id = account_spot.id

    for bot in bots:
        symbol = bot[1].replace('/', '')
        trade_client = TradeClient(api_key=huobi_api_key, secret_key=huobi_secret_key)
        trade_list = trade_client.get_open_orders(symbol=symbol, account_id=account_id)

        if [obj.id for obj in trade_list]:
            bot[2] = 'working'
            bot[3] = 'normal'
        else:
            bot[2] = 'waiting'
            bot[3] = 'normal'


    # Signals

    for new_pair in mf_list:
        working_pair = 'N'
        waiting_filter = 'N'
        for bot in bots:
            if bot[1] == new_pair and bot[2] == 'working':
                working_pair = 'Y'
                break
            elif bot[1] == new_pair and bot[3] == 'filter':
                waiting_filter = 'Y'
                break
        if working_pair == 'N' and waiting_filter == 'N':
            for bot in bots:
                if bot[2] == 'waiting' and bot[3] == 'normal':
                    bot[1] = new_pair
                    data['bot_id'] = bot[0]
                    data['pair'] = new_pair
                    send_data = json.dumps(data)
                    response = requests.post(url, data=send_data, headers=newHeaders)

                    print(data['bot_id'], data['pair'])
                    print(response.status_code)
                    print(response.text)
                    print(response.raise_for_status())
                    print(send_data)
                    print('-' * 60)
                    print(mf_list)
                    s = [[str(e) for e in row] for row in bots]
                    lens = [max(map(len, col)) for col in zip(*s)]
                    fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
                    table = [fmt.format(*row) for row in s]
                    print('\n'.join(table))
                    print('=' * 60)

                    if response.ok:
                        bot[3] = 'filter'
                    break
    print(mf_list)
    s = [[str(e) for e in row] for row in bots]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
    table = [fmt.format(*row) for row in s]
    print('\n'.join(table))
    print('-' * 60)

    cmc_req_cnt += 1

    start = time.perf_counter()
    stop = 0
    while True:
        stop = time.perf_counter()
        if stop - start > 300:
            break