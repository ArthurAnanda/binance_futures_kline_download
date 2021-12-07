'''
币安永续合约k线数据下载服务
作者：Arthur
说明：通过api: https://api.binance.com/api/v1/klines 下载币安永续合约k线分钟数据，支持分钟，小时等K线数据，在每次下载之后通过计算下一次数据开始时间点的方法解决数据重复或者缺漏问题。
本脚本为下载程序入口，将k线数据通过多线程方法下载并保存为本地的feather文件
注意：所得数据所有时间为东八区时间，下载过程中需要打开全局代理，pandas版本：0.25.1以上版本，
'''
from utils import *

MySet.pd_set(20, 20)

coin_list = BinanceFuturesCoinList[:10] # 下载数据币种列表，以前10个币种为例

print(coin_list)
interval = '15m'  # 以15分钟数据为例
file_prefix = f'bi_f_kline_test'  # 文件名前缀
err_list = []
dir = ''  # feather 文件保存路径，默认空值为当前路径

time_start = '2021-01-01'  # 数据开始时间点
time_end = '2021-10-30'  # 数据结束时间点


def load_coin(coin):
    info_start = f'{coin} start'
    info_finish = f'{coin} finish'
    print(info_start)
    try:
        BinanceFuturesKlineSave.kline_to_feather(file_prefix=file_prefix, coin=coin, interval=interval, time_start=time_start, time_end=time_end, dir=dir, sleep_seconds=0, verbose_out=True)
        print(info_finish)
    except:
        info = traceback.format_exc()
        print(info)
        info_err = f'{coin} err'
        print(info_err)
        err_list.append(coin)


for coin in coin_list:
    # FuncTool.thread_run(func=lambda: load_coin(coin=coin)) # 多个币种同时多线程下载容易触发频限
    load_coin(coin=coin)
