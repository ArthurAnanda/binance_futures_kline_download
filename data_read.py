from utils import *

MySet.pd_set(20, 20)

coin_list = BinanceFuturesCoinList[:10]
print(coin_list)
interval = '15m'
file_prefix = 'bi_f_kline_test'
dir = ''


def read_coin_kline(coin):
    fname = f'{file_prefix}_{interval}_{coin}'
    if dir:
        fname = dir + '/' + fname
    data = pd.read_feather(path=fname)
    return data


def check_data_complete(data):
    '''
    如果k线数据是完整连续的，返回True
    :param data:
    :return:
    '''
    return len(data['time'].diff().value_counts()) == 1


data_list_dict = {coin: read_coin_kline(coin=coin) for coin in coin_list}

print(data_list_dict)
data_complete_dict = {coin: check_data_complete(data=data_list_dict[coin]) for coin in coin_list}
print(data_complete_dict)
print('err data: ', {coin: data_complete_dict[coin] for coin in coin_list if data_complete_dict[coin] == False})
