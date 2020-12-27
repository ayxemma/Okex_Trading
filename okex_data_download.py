import datetime
import os

from datetime import datetime, timezone, timedelta
import time
import pandas as pd
import func

def get_hist_minutes_kline(context, n_minutes, api_connect, inst_id, endtime=datetime.now(timezone.utc), allcolumns_flag=False):
    """
    pull minute level price and volume data for the given duration
    """
    i = 1
    kline_hist = []
    tstamp = endtime
    while 300 * i <= n_minutes + 300:
        if i%10==0:
            time.sleep(5)
        startdelta = min(300 * i, n_minutes)
        enddelta = min(300 * (i - 1), n_minutes)
        t_hist = api_connect.get_kline(instrument_id=inst_id,
                                     start=(tstamp - timedelta(minutes=startdelta)).strftime(context.isoformat),
                                     end=(tstamp - timedelta(minutes=enddelta)).strftime(context.isoformat), granularity=60)
        kline_hist = kline_hist + t_hist
        i += 1
    kline_hist = pd.DataFrame(kline_hist)
    if allcolumns_flag:
        kline_hist = kline_hist.iloc[:,[0, 1,2,3,4, -1]]
        kline_hist.columns = ['date', 'open','high','low','close', 'volume']
        kline_hist[['open','high','low','close', 'volume']] = kline_hist[['open','high','low','close', 'volume']].apply(pd.to_numeric)
    else:
        kline_hist = kline_hist.iloc[:, [0, 4, -1]]
        kline_hist.columns = ['date', 'price', 'volume']
        kline_hist[['price', 'volume']] = kline_hist[['price', 'volume']].apply(pd.to_numeric)

    kline_hist['date']=pd.to_datetime(kline_hist['date'])
    kline_hist=kline_hist.sort_values(by=['date'], ascending=True)
    return kline_hist

    
def update_data_file(fname, api_connect, inst_id):
    data_hist_all=pd.read_pickle(fname)
    data_hist=get_hist_minutes_kline(context, 5000, api_connect, inst_id, endtime=datetime.now(timezone.utc), allcolumns_flag=True)
    data_hist_all=data_hist_all.append(data_hist)
    data_hist_all=data_hist_all.drop_duplicates(subset=['date'])
    data_hist_all = data_hist_all.reset_index(drop=True)
    data_hist_all.to_pickle(fname)
    
context=func.Context()
context = func.init_context(context)
context = func.connect_context(context, ws_flag=True, restapi_flag=True)
fname=f'./data/data_hist_{context.instrument_type}_{context.instrument_id}.pkl'
update_data_file(fname, context.futureAPI, context.instrument_id)
fname=f'./data/data_hist_{context.spot_inst_type}_{context.spot_inst_id}.pkl'
update_data_file(fname, context.spotAPI, context.spot_inst_id)


