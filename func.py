import time
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import okex.futures_api as future
import okex.spot_api as spot
import okex.websocket as websocket
import matplotlib.pyplot as plt
import json
import random
import pickle
import logging
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


logger = logging.getLogger(__name__)

class Context:
    pass

def init_context(context):
    logger.info('Initiate context')
    context.instrument_id = 'ETH-USD-210326'
    context.spot_inst_id = 'ETH-USDT'
    context.instrument_type = 'futures'
    context.spot_inst_type = 'spot'
    context.records = []
    context.order_enter_flag = False
    context.printflag = True
    context.strat = 'crypto_orderfollow'
    context.orderobj = OrderConfig()
    context.printinterval= 3 # minutes
    context.api_key = "xxxx"
    context.secret_key = "xxxx"
    context.passphrase = 'xxxx'
    context.isoformat = "%Y-%m-%dT%H:%M:%SZ"
    context.orderidformat="ordy%Ym%md%dh%Hm%Mms%f"
    context.vol_stat = {}
    return context

def connect_context(context, ws_flag=True, restapi_flag=True):
    """
    set up context object
    :param context: context is the object to hold all trade stats across functions
    :return: context
    """
    logger.info('Connect to websocket and restapi')
    if restapi_flag:
        context.futureAPI = future.FutureAPI(context.api_key, context.secret_key, context.passphrase, True)
        context.spotAPI = spot.SpotAPI(context.api_key, context.secret_key, context.passphrase, True)
    if ws_flag:
        context.ws = websocket.WSSubscription(context.instrument_id, context.instrument_type)
        context.ws_spot = websocket.WSSubscription(context.spot_inst_id, context.spot_inst_type)
        time.sleep(3)
    return context

def init_kline(context):
    logger.info("Setup history kline data")
    context, data_hist = setup_hist_kline(context)
    curr_tday=data_hist['date'].iloc[-1]
    return context, data_hist, curr_tday

class OrderConfig:
    """
    Order class which contains order details
    """
    def __init__(self):
        self.size=10
        self.leverage=5
        self.match_price=1
        self.order_type=0
        


def setup_hist_kline(context):
    data_hist = get_kline_file(context, allcolumns_flag=False)
    curr_tday = data_hist['date'].iloc[-1]
    context.tday = context.ws.GetTimeStamp()
    passed_time = (context.tday - curr_tday).seconds // 60 + 1
    data_hist_added = get_hist_minutes_kline(context, passed_time)
    data_hist_added = data_hist_added.loc[data_hist_added['date'] > data_hist['date'].max()]
    data_hist = data_hist.append(data_hist_added)
    candle_hist = context.ws_spot.GetHistCandle()
    candle_hist = candle_hist.loc[candle_hist['date']>data_hist['date'].max()]
    data_hist = data_hist.append(candle_hist)
    data_hist = data_hist.reset_index(drop=True)
    return context, data_hist

def save_kline_file(context, data_hist):
    fname = f'./data/data_hist_{context.spot_inst_type}_{context.spot_inst_id}_current.pkl'
    data_hist.to_pickle(fname)
    
def get_kline_file(context, allcolumns_flag = False):
    fname = f'./data/data_hist_{context.spot_inst_type}_{context.spot_inst_id}_current.pkl'
    data_hist=pd.read_pickle(fname)
#    data_hist = data_hist[['date', 'close', 'volume']]
#    data_hist = data_hist.rename(columns={'close':'price'})
#    return data_hist[['date', 'price', 'volume']].iloc[-2880:]
    return data_hist
    
def get_hist_minutes_kline(context, n_minutes, endtime=datetime.now(timezone.utc), allcolumns_flag=False):
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
        t_hist = context.spotAPI.get_kline(instrument_id=context.spot_inst_id,
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
        kline_hist = kline_hist.iloc[:,[0, 4, -1]]
        kline_hist.columns = ['date', 'price', 'volume']
        kline_hist[['price', 'volume']] = kline_hist[['price', 'volume']].apply(pd.to_numeric)

    kline_hist['date']=pd.to_datetime(kline_hist['date'])
    kline_hist=kline_hist.sort_values(by=['date'], ascending=True)
    return kline_hist

def check_ws_connected(context):
    localtime = pd.to_datetime(datetime.now(timezone.utc)).tz_convert(None)  
    wstime = context.ws.GetTimeStamp().tz_convert(None)
    if localtime > wstime:
        timediff = (localtime - wstime).seconds
    else:
        timediff = (localtime - wstime).seconds
    if timediff >=100:
        logger.warning(f'Websocket is lagged by {timediff} seconds.')
        return False
    else:
        return True

def update_data_hist(context, data_hist):
    new_candle = context.ws_spot.GetHistCandle()
    if new_candle.shape[0]>=2880:
        return new_candle.iloc[-2880:].reset_index(drop=True)
    else:
        data_hist = data_hist.loc[data_hist['date']< new_candle['date'].iloc[0]]
        data_hist = data_hist.append(new_candle)
        return data_hist.iloc[-2880:].reset_index(drop=True)
def get_hist_minutes_kline_spot(context, n_minutes, endtime=datetime.now(timezone.utc), allcolumns_flag=False):
    i = 1
    kline_hist = []
    tstamp = endtime
    while 300 * i <= n_minutes + 300:
        if i%10==0:
            time.sleep(5)
        startdelta = min(300 * i, n_minutes)
        enddelta = min(300 * (i - 1), n_minutes)
        t_hist = context.spotAPI.get_kline(instrument_id=context.instrument_id,
                                     start=(tstamp - timedelta(minutes=startdelta)).strftime(context.isoformat),
                                     end=(tstamp - timedelta(minutes=enddelta)).strftime(context.isoformat), granularity=60)
        kline_hist = kline_hist + t_hist
        i += 1
    kline_hist = pd.DataFrame(kline_hist)
    if allcolumns_flag:
        kline_hist.columns = ['date', 'open','high','low','close', 'volume']
        kline_hist[['open','high','low','close', 'volume']] = kline_hist[['open','high','low','close', 'volume']].apply(pd.to_numeric)
    else:
        kline_hist = kline_hist[[0, 4, 6]]
        kline_hist.columns = ['date', 'price', 'volume']
        kline_hist[['price', 'volume']] = kline_hist[['price', 'volume']].apply(pd.to_numeric)

    kline_hist['date']=pd.to_datetime(kline_hist['date'])
    kline_hist=kline_hist.sort_values(by=['date'], ascending=True)
    return kline_hist

def get_trade_cost(context):
    """
    get the average cost of the position for long/short
    """
    if context.portfolio_amount>0:
        cost = float(context.positions['holding'][0]['long_avg_cost'])
    else:
        cost = float(context.positions['holding'][0]['short_avg_cost'])
    return cost

def generate_oid(id_format, ordernumber=1):
    if ordernumber==1:
        return datetime.now(timezone.utc).strftime(id_format)
    else:
        id_base=datetime.now(timezone.utc).strftime(id_format)
        id_lst=[id_base + str(i) for i in range(ordernumber)]
        return id_lst

def create_market_order(context, otype, orderamount, current_price, order_type, data_hist, vol_hist, add_msg = ''):
    """
    create market order and record the order stats
    """
    if orderamount==0:
        return context
    if ((otype ==1 and context.portfolio_amount>0) or (otype ==2 and context.portfolio_amount<0)):
        return context
    # order_id=generate_oid(context.orderidformat)
    # context.futureAPI.take_order(order_id, context.instrument_id, str(otype), str(current_price), str(orderamount), str(context.orderobj.match_price), str(context.orderobj.leverage))
    logger.info(f"Create market order with otype (1:buy, 2:sell): {otype}")
    order_id = generate_oid(context.orderidformat, 1)
    algo_order_one_order(context, orderamount, otype)
    send_email(context, order_type, data_hist, vol_hist, add_msg)
    return context

def get_portfolio_position(context):
    context.positions=context.futureAPI.get_specific_position(context.instrument_id)
    context.portfolio_long=float(context.positions['holding'][0]['long_avail_qty'])
    context.portfolio_short=float(context.positions['holding'][0]['short_avail_qty'])
    context.portfolio_amount = context.portfolio_long-context.portfolio_short
    return context

def sanity_check(context):
    """
    check if there are long and short positions at the same time
    """
    if context.portfolio_long*context.portfolio_short>0:
        print('Error: both long and short positions exist!')

def create_order_data(p_lst, id_lst, amount_lst, otype):
    order_lst = []
    for i in range(len(id_lst)):
        order_t = {"client_oid": id_lst[i], "type": str(otype), "price": str(p_lst[i]), "size": str(amount_lst[i]), "match_price": "0"}
        order_lst.append(order_t)
    orders_data = json.dumps(order_lst)
    return orders_data

def algo_order(context, orderamount, otype):
    """
    :param context:
    :param orderamount:
    :param otype: 	1:open long 2:open short 3:close long 4:close short
    :return:
    """
    orderamount_total = orderamount
    if otype in [1, 3] :
        pos = 'long_avail_qty'
    else:
        pos = 'short_avail_qty'
    while orderamount>0:
        current_price=context.ws.GetMarkPrice()
        orderbook = context.ws.GetDepth()
        # generate price list
        p_min_sell = float(orderbook['asks'][-1][0])
        p_max_buy = float(orderbook['bids'][0][0])
        p_range = p_min_sell-p_max_buy
        ordernumber = min(int(p_range / 0.001), 5)
        if otype in [1, 4]:
            p_lst = [p_max_buy+0.001*i for i in range(1, ordernumber+1)]
        else:
            p_lst = [p_min_sell - 0.001 * i for i in range(1, ordernumber + 1)]
        # generate amount list
        amount_lst = [random.random()+0.1 for i in range(ordernumber)]
        amount_lst = [max(int(amount_lst[i]/sum(amount_lst)*orderamount),1) for i in range(ordernumber)]
        # generate order id list
        id_lst = generate_oid(context.orderidformat, ordernumber)
        # generate order list
        orders_data = create_order_data(p_lst, id_lst, amount_lst, otype)
        # place orders
        result = context.futureAPI.take_orders(context.instrument_id, orders_data=orders_data, leverage=context.orderobj.leverage)
        time.sleep(1)
        # get transactions
        # fills=context.futureAPI.get_order_list(status=-1, instrument_id=context.instrument_id, froms=None, to=None, limit=100)
        # cancel remaining orders
        context.futureAPI.revoke_orders(context.instrument_id, client_oids=id_lst)
        # get filled positions
        holdings=int(context.futureAPI.get_specific_position(context.instrument_id)['holding'][0][pos])
        print(f'Filled holdings is {holdings}')
        if otype in [1, 2]:
            orderamount = orderamount_total - holdings
        else:
            orderamount = holdings
        print(f'Left order amount is {orderamount}')

def algo_order_one_order(context, orderamount, otype):
    """
    :param context:
    :param orderamount:
    :param otype: 	1:open long 2:open short 3:close long 4:close short
    :return:
    """
    orderamount_total = orderamount
    if otype in [1, 3] :
        pos = 'long_avail_qty'
    else:
        pos = 'short_avail_qty'
    current_price=context.ws.GetMarkPrice()
    while orderamount>0:
        
        # generate order id list
        order_id = generate_oid(context.orderidformat, 1)
        last_trade_price = context.ws.GetLastTradePrice()
        last_trade_side = context.ws.GetLastTradeSide()
        # generate price list
        if otype in [1, 4]:
            p = last_trade_price+ 0.1
        else:
            p = last_trade_price - 0.1

        # generate order list
        order_t = [{"client_oid": order_id, "type": str(otype), "price": str(p), "size": str(orderamount),
                    "match_price": "0"}]
        orders_data = json.dumps(order_t)
        # place orders
        result = context.futureAPI.take_orders(context.instrument_id, orders_data=orders_data, leverage=context.orderobj.leverage)
        time.sleep(2)
        # get transactions
        # fills=context.futureAPI.get_order_list(status=-1, instrument_id=context.instrument_id, froms=None, to=None, limit=100)
        # cancel remaining orders
        context.futureAPI.revoke_orders(context.instrument_id, client_oids=[order_id])
        # get filled positions
        holdings=int(context.futureAPI.get_specific_position(context.instrument_id)['holding'][0][pos])
        # print(f'Filled holdings is {holdings}')
        if otype in [1, 2]:
            orderamount = orderamount_total - holdings
        else:
            orderamount = holdings
        # print(f'Left order amount is {orderamount}')

def market_order_exit(context, data_hist, vol_hist, ordertype):
    """
    close positions at the given direction at market price if there's portfolio holdings
    """

    if context.portfolio_amount!=0:
        # context.futureAPI.market_close(context.instrument_id, context.direction)
        if (((context.portfolio_amount>0) and (context.orderside=='long')) or ((context.portfolio_amount<0) and (context.orderside=='short'))):
            return context
        elif (context.portfolio_amount>0):
            algo_order_one_order(context, context.portfolio_long, 3)
        else:
            algo_order_one_order(context, context.portfolio_short, 4)
        logger.info(f"Market order exit with order type: {ordertype}")
        send_email(context, ordertype, data_hist, vol_hist)
        context = record_context_recording(context, data_hist, np.nan, np.nan, np.nan, np.nan, -context.portfolio_amount, ordertype)
        context.portfolio_amount = 0
    return context

def record_context_recording(context, data_hist, long_pctchg, short_pctchg, max_decline, max_increase, pos_enter,
                             ordertype):
    """
    record order details
    """
    record_dict = {'time': context.tday, 'price': context.ws.GetMarkPrice(), 'portfolio': context.portfolio_amount,
                   'current_volume': data_hist['volume'].iloc[-1],
                   'median_volume': data_hist['volume'][:-1].median(),
                   'Median+4*std vol': data_hist['volume'][:-1].median() + 4 * data_hist['volume'][:-1].std(),
                   'long_pctchange': long_pctchg, 'short_pctchange': short_pctchg,
                   'max_decline': max_decline, 'max_increase': max_increase,
                   'pos_enter': pos_enter, 'ordertype': ordertype}
    context.records.append(record_dict)
    return context

def send_email(context, ordertype, data_hist, vol_hist, add_msg='No additional message'):
    logger.info('send email')
    msgstr1, msgstr2 = debug_print(context, data_hist, vol_hist, flag_graph=False)
    me = 'xxxx@gmail.com'
    password = 'xxxx'
    server = 'smtp.gmail.com:587'
    you = 'xxxx@gmail.com'
    
    html = f"""
    <html><body>
    <p>Time: {context.tday}</p>
    <p>Portfolio position: {context.portfolio_amount}</p>
    <p>{msgstr1}</p>
    <p>{msgstr2}</p>
    <p>{add_msg}</p>
    {{table}}
    </body></html>
    """ 

    html = html.format(table=tabulate(data_hist.tail(15), headers=data_hist.columns.values, tablefmt="html"))
    
    message = MIMEMultipart(
        "alternative", None, [MIMEText(html,'html')])
    
    message['Subject'] = f"TradeAlert: ordertype is {ordertype}"
    message['From'] = me
    message['To'] = you
    server = smtplib.SMTP(server)
    server.ehlo()
    server.starttls()
    server.login(me, password)
    server.sendmail(me, you, message.as_string())
    server.quit()

def send_email_message(msg):
    logger.info('send email message')
    me = 'wintersunrise11@gmail.com'
    password = '920301Ayx11!!'
    server = 'smtp.gmail.com:587'
    you = 'wintersunrise11@gmail.com'
    
    html = f"""
    <html><body>
    <p>{msg}</p>
    </body></html>
    """ 
    
    message = MIMEMultipart(
        "alternative", None, [MIMEText(html,'html')])
    
    message['Subject'] = f"TradeMsg :{msg}"
    message['From'] = me
    message['To'] = you
    server = smtplib.SMTP(server)
    server.ehlo()
    server.starttls()
    server.login(me, password)
    server.sendmail(me, you, message.as_string())
    server.quit()

def time_track_market_order(context, data_hist):
    """
    track the duration of order entering, exit if too long
    """
    if context.portfolio_amount!=0:
        order_create_time = pd.to_datetime(context.positions['holding'][0]['updated_at'], utc= True)
        if (context.ws.GetTimeStamp() - order_create_time).seconds / 60 / 60 >= 12: # greater than 12 hours
            context = market_order_exit(context, data_hist, 'timetrackexit')
    return context

def save_data(data, filedir):
    with open(filedir, 'wb') as f:
        pickle.dump(data, f)

def debug_print(context, data_hist, vol_hist, flag_graph=False):
    """
    print statistics and visualization if set printflag=True in Context object
    """
    logger.info(f'***********Every{context.printinterval}Mins**********')
    price_hist = data_hist.iloc[-360:]
    price_hist=price_hist.set_index('date')
    if flag_graph:
        price_hist['price'].plot(style='b*-')
        plt.show()
        price_hist['volume'].plot(style='b*-')
        plt.show()
    logger.info(context.tday)
    logger.info(f'portfolio position is: {context.portfolio_amount}')
    logger.info('current price is %f' % context.current_price)
    msgstr1='Current %f; Median %f; Median+4std vol %f' % (round(vol_hist['volume'].iloc[-1],1), round(vol_hist['volume'][:-1].median(),1), round(vol_hist['volume'][:-1].median()+4*vol_hist['volume'][:-1].std()))
    logger.info(msgstr1)
    p_hist=price_hist['price'].iloc[-30:].resample('15Min').mean()
    long_pctchg=p_hist.pct_change().values[-1]
    p_hist_short=price_hist['price'].iloc[-9:].resample('3Min').mean()
    short_pctchg=p_hist_short.pct_change().values[-1]
    msgstr2='long_pctchange: %f, short_pctchange: %f' % (long_pctchg, short_pctchg)
    logger.info(msgstr2)
    return msgstr1, msgstr2
    
#    print('past 5 minutes data_hist')
#    print(data_hist.tail())
