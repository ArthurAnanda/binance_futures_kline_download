import os, sys, time, json, requests, traceback, socket
import pandas as pd
import datetime as dt
from threading import Thread

HEADERS = {'accept'                   : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
           'accept-encoding'          : 'gzip, deflate, br',
           'accept-language'          : 'zh-CN,zh;q=0.9,en;q=0.8',
           'cache-control'            : 'max-age=0',
           'sec-fetch-mode'           : 'navigate',
           'sec-fetch-site'           : 'none',
           'sec-fetch-user'           : '?1',
           'upgrade-insecure-requests': '1',
           'user-agent'               : 'Firefox/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}

BinanceFuturesCoinList = ['btc', 'eth', 'bch', 'xrp', 'eos', 'ltc', 'trx', 'etc', 'link', 'xlm', 'ada', 'xmr', 'dash', 'zec', 'xtz', 'bnb', 'atom', 'ont', 'iota', 'bat', 'vet', 'neo', 'qtum', 'iost', 'theta', 'algo', 'zil', 'knc', 'zrx', 'comp', 'omg', 'doge', 'sxp', 'kava', 'band', 'rlc', 'waves', 'mkr', 'snx', 'dot', 'defi', 'yfi', 'bal', 'crv', 'trb', 'yfii', 'rune', 'sushi', 'srm', 'bzrx', 'egld', 'sol', 'icx', 'storj', 'blz', 'uni', 'avax', 'ftm', 'hnt', 'enj', 'flm', 'tomo', 'ren',
                          'ksm', 'near', 'aave', 'fil', 'rsr', 'lrc', 'matic', 'ocean', 'cvc', 'bel', 'ctk', 'axs', 'alpha', 'zen', 'skl', 'grt', '1inch', 'akro', 'chz', 'sand', 'ankr', 'luna', 'bts', 'lit', 'unfi', 'dodo', 'reef', 'rvn', 'sfp', 'xem', 'coti', 'chr', 'mana', 'alice', 'hbar', 'one', 'lina', 'stmx', 'dent', 'celr', 'hot', 'mtl', 'ogn', 'btt', 'nkn', 'sc', 'dgb', '1000shib', 'icp', 'bake', 'gtc', 'btcdom', 'keep', 'tlm', 'iotx', 'audio', 'ray', 'c98', 'mask', 'ata', 'dydx',
                          '1000xec', 'gala', 'celo', 'ar', 'klay', 'arpa', 'nu', 'ctsi', 'lpt', 'ens']


class Adorn:
    @staticmethod
    def run_ensure(retry_times=10, traceback_print=False, print_retry=False, sleep=0):
        '''
        try to run function till success
        :param retry_times: times of try to run function
        :param lc_utc: local time and utc time gap
        :param traceback_print: print traceback or not
        :param sleep: time sleep between two trying.
        :return:
        '''

        def run_function_to(func):
            def wrapper(*args, **kwargs):
                retry = 0
                while True:
                    if retry > retry_times:
                        if traceback_print:
                            raise ConnectionError(f'try [{func.__name__}] {retry_times} times failed. traceback:\n{exc}')
                        else:
                            raise ConnectionError(f'try [{func.__name__}] {retry_times} times failed.')
                    try:
                        result = func(*args, **kwargs)
                        return result
                    except:
                        if traceback_print:
                            exc = traceback.format_exc()
                            print(f'run_ensure retry [{func.__name__}]..[{args}]..[{kwargs}]..{Write.time()}\ntraceback:\n{exc}')
                        else:
                            if print_retry:
                                print(f'retry... {retry + 1}')
                        retry += 1
                    time.sleep(sleep)

            return wrapper

        return run_function_to


class MySet:
    @staticmethod
    def pd_set(rows=50, cols=50, round=5):
        pd.options.display.min_rows = None
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('expand_frame_repr', False)
        pd.set_option('max_rows', rows)
        pd.set_option('max_columns', cols)
        pd.set_option('display.float_format', lambda x: f'%.{round}f' % x)


class Write:
    @staticmethod
    def pc_name():
        name = socket.gethostname()
        return name

    @staticmethod
    def file_name():
        return os.path.abspath(sys.argv[0])

    @staticmethod
    def script_name():
        file_name = Write.file_name()
        script = file_name.split('/')[-1].split('\\')[-1]
        return script

    @staticmethod
    def str_reformat(string):
        return string.replace("/", "_").replace(":", "_").replace("-", "_").replace(" ", "_").lower()

    @staticmethod
    def today(lc_utc=8):
        return str(dt.datetime.now() + dt.timedelta(hours=8 - lc_utc))[:10]

    @staticmethod
    def time(lc_utc=8):
        return Write.str_reformat(str(dt.datetime.now() + dt.timedelta(hours=8 - lc_utc))[:19])


class FuncTool:
    @staticmethod
    def thread_run(func, name=''):
        t = Thread(target=func, name=name)
        t.start()


class TimeBarCalTool:
    @staticmethod
    def seconds_interval(interval):
        unit = interval[-1:]
        num = int(interval[:-1])
        if unit == 'm':
            result = dt.timedelta(minutes=num).total_seconds()
        elif unit == 'h':
            result = dt.timedelta(hours=num).total_seconds()
        elif unit == 'd':
            result = dt.timedelta(days=num).total_seconds()
        elif unit == 'w':
            result = dt.timedelta(weeks=num).total_seconds()
        else:
            raise ValueError('interval parameter does not support.')
        return result

    @staticmethod
    def time_delta_cal(time_frame):
        frame = time_frame[-1]
        unit = int(time_frame[:-1])
        if frame == 'm':
            return dt.timedelta(minutes=unit)
        elif frame == 'h':
            return dt.timedelta(hours=unit)
        elif frame == 'd':
            return dt.timedelta(days=unit)
        elif frame == 'w':
            return dt.timedelta(weeks=unit)
        else:
            raise ValueError('time_frame parameter does not support.')

    @staticmethod
    def time_last_complete_bar(time_frame='1m'):
        '''
        :param lc_utc:
        :return: shanghai time of last_complete_bar
        '''
        assert time_frame[-1] == 'm', 'time_frame must be minutes.'
        time_frame = int(time_frame[:-1])
        time_now = dt.datetime.now()
        time_last_run_minute = (time_now.minute // time_frame) * time_frame
        time_last_run = dt.datetime(year=time_now.year, month=time_now.month, day=time_now.day, hour=time_now.hour, minute=time_last_run_minute)
        this_bar_start = time_last_run - dt.timedelta(minutes=time_frame)
        return this_bar_start

    @staticmethod
    def time_end_bar(time_start, interval, bars_num_total):
        sec_one = TimeBarCalTool.seconds_interval(interval=interval)
        time_end = pd.to_datetime(time_start) + dt.timedelta(seconds=sec_one) * (bars_num_total - 1)
        return time_end

    @staticmethod
    def time_start_bar(time_end, interval, bars_num_total):
        sec_one = TimeBarCalTool.seconds_interval(interval=interval)
        time_end = pd.to_datetime(time_end) - dt.timedelta(seconds=sec_one) * (bars_num_total - 1)
        return time_end

    @staticmethod
    def millisec_utc_sh_time_str(time_str):
        '''
        :param time_str: shanghai time string
        :return: utc millisecond
        '''
        utc_time = pd.to_datetime(time_str) - dt.timedelta(hours=8)
        utc_milliseconds = int(utc_time.timestamp() * 1000)
        return utc_milliseconds

    @staticmethod
    def sec_utc_sh_time_str(time_str):
        '''
        :param time_str: shanghai time string
        :return: utc millisecond
        '''
        utc_time = pd.to_datetime(time_str) - dt.timedelta(hours=8)
        utc_seconds = int(utc_time.timestamp())
        return utc_seconds


class BinanceFuturesKlineGet:
    def __init__(self, coin, timeout=3):
        self.symbol = f'{coin}usdt'
        self.timeout = timeout
        self._headers = HEADERS

    @Adorn.run_ensure(retry_times=20, traceback_print=False, sleep=0)
    def _requests_get(self, url, ip_ban_safe=False):
        res = requests.get(url=url, timeout=self.timeout, headers=self._headers)
        info = json.loads(s=res.text)
        if isinstance(info, dict):
            if 'code' in info.keys() and 'msg' in info.keys():
                print('_request_till may err:', url)
                print(info)
                if ip_ban_safe:
                    if 'Too many requests' in info['msg']:
                        time.sleep(20)
                        raise ConnectionError('Too many requests')
                    if 'Way too many requests' in info['msg'] or 'banned until' in info['msg']:
                        time.sleep(60 * 2)
                        raise ConnectionError('IP ban!')
                    print('please wait......')
        return info

    def _parse_content(self, content):
        data = pd.DataFrame(content).iloc[:, :-1]
        columns = ['time', 'open', 'high', 'low', 'close', 'vol', 'time_close_utc_stamp', 'amt', 'trades', 'buy_vol', 'buy_amt']
        f_col = ['open', 'high', 'low', 'close', 'vol', 'amt', 'buy_vol', 'buy_amt']
        data.columns = columns
        data[f_col] = data[f_col].astype(float)
        data['time'] = pd.to_datetime(data['time'], unit='ms') + dt.timedelta(hours=8)
        data.drop(columns=['time_close_utc_stamp'], inplace=True)
        data['buy_ratio_vol'] = data['buy_vol'] / data['vol']
        data['buy_ratio_amt'] = data['buy_amt'] / data['amt']
        return data

    @Adorn.run_ensure(retry_times=20)
    def get_kline_from_time(self, interval, time_start, limit):
        utc_start = TimeBarCalTool.millisec_utc_sh_time_str(time_str=time_start)
        url = f'https://fapi.binance.com/fapi/v1/klines?symbol={self.symbol.replace("/", "")}&interval={interval}&startTime={utc_start}&limit={limit}'
        content = self._requests_get(url=url, ip_ban_safe=True)
        return self._parse_content(content=content)


class BinanceFuturesKlineSave:
    @staticmethod
    def kline_to_feather(file_prefix, coin, interval, time_start, dir='', time_end='', sleep_seconds=0.1, verbose_out=True, limit=1500):
        data_save = pd.DataFrame()
        bi = BinanceFuturesKlineGet(coin=coin)
        if not time_end:
            time_end = TimeBarCalTool.time_last_complete_bar(time_frame=interval)
        else:
            time_end = pd.to_datetime(time_end)
        data = bi.get_kline_from_time(interval=interval, time_start=time_start, limit=limit)
        last_time = data['time'].iloc[-1]
        data_save = data_save.append(data)
        while True:
            time.sleep(sleep_seconds)
            data = bi.get_kline_from_time(interval=interval, time_start=last_time + TimeBarCalTool.time_delta_cal(time_frame=interval), limit=limit)
            data_save = data_save.append(data)
            if verbose_out:
                print(data)
            print(coin)
            last_time = data['time'].iloc[-1]
            if last_time >= time_end:
                break

        data_save.reset_index(drop=True, inplace=True)
        fname = f'{file_prefix}_{interval}_{coin}'
        if dir:
            fname = dir + '/' + fname
        data_save.to_feather(fname=fname)
