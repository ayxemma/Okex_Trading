#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 23:13:05 2019

@author: ayx
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# encoding: utf-8
# 
# Market Real-time Subscription v3
#
# Copyright 2019 FawkesPan
#
# Do What the Fuck You Want To Public License
#

import os
import time
import ssl
import sys
import code
import json
import hashlib
import hmac
import urllib
import threading
import websocket
import zlib
import string
from datetime import datetime, timezone, timedelta
import requests
import base64
import dateutil.parser as dp
import pandas as pd
import pickle


try:
    import readline
except ImportError:
    pass

pong = time.time()



def unix_time_seconds(dt):
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
    return (dt - epoch).total_seconds()

class WSSubscription:

    def __init__(self, instrument_id='BTC-USD-190517', market='futures', on_message=None):
        self.__iid = instrument_id
        self.__market = market
        self.__Depth = {}
        self.__mark_price={}
        self.__candle=[]
        self.__allcandle=[]
        self.__allcandle_df=pd.DataFrame()
        self.__long_open_outstanding=0
        self.__short_open_outstanding=0
        self.__positions=0
        self.__timestamp=0
        self.__last_trade_side = None
        self.__last_trade_price = None
        self.api_key = "xxxx"
        self.secret_key = "xxxx"
        self.passphrase = 'xxxx'
        if on_message is not None:
            self.__callbackEnabled = True
            self.__callback = on_message
        else:
            self.__callbackEnabled = False

        thread = threading.Thread(target=self.sub, args=())
        thread.daemon = True
        thread.start()
    def get_server_time(self):
        url = "http://www.okex.com/api/general/v3/time"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['iso']
        else:
            return ""
    def server_timestamp(self):
        server_time = self.get_server_time()
        parsed_t = dp.parse(server_time)
        timestamp = parsed_t.timestamp()
        return timestamp
    def buildMySign(self):
        timestamp = str(self.server_timestamp())
        message = str(timestamp) + 'GET' + '/users/self/verify'
        mac = hmac.new(bytes(self.secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        sign = base64.b64encode(d)
#        return timestamp, sign
        login_param = {"op": "login", "args": [self.api_key, self.passphrase, timestamp, sign.decode("utf-8")]}
        login_str = json.dumps(login_param)
        return login_str
    def GetDepth(self):
        return self.__Depth
    def GetMarkPrice(self):
        return self.__mark_price
    def GetTimeStamp(self):
        return self.__timestamp
    def Get1MinCandles(self):
        if type(self.__candle) == list:
            self.__candle = pd.DataFrame(self.__candle).T
            self.__candle = self.__candle.iloc[:,[0,4,-1]]
            self.__candle.columns = ['date', 'price', 'volume']
            self.__candle['date'] = pd.to_datetime(self.__candle['date'])
            self.__candle[['price', 'volume']] = self.__candle[['price', 'volume']].apply(pd.to_numeric)
        return self.__candle
    def GetHistCandle(self):
        self.__allcandle_df=pd.DataFrame(self.__allcandle)
        self.__allcandle_df=self.__allcandle_df.iloc[:,[0,4,-1]]
        self.__allcandle_df.columns = ['date', 'price', 'volume']
        self.__allcandle_df['date'] = pd.to_datetime(self.__allcandle_df['date'])
        self.__allcandle_df[['price', 'volume']] = self.__allcandle_df[['price', 'volume']].apply(pd.to_numeric)
        return self.__allcandle_df
    def GetLastMinVolume(self):
        return float(self.__allcandle[-1][-1])
    def GetLongOpenPosition(self):
        return self.__long_open_outstanding
    def GetShortOpenPosition(self): 
        return self.__short_open_outstanding
    def GetPosition(self):
        return self.__positions
    def GetRawCandle(self):
        return self.__candle
    def GetLastTradePrice(self):
        return self.__last_trade_price
    def GetLastTradeSide(self):
        return self.__last_trade_side
    def subscribe(self, ws):        
        def operator(op, args):
            message = {
                'op': op,
                'args': args
            }
            ws.send(json.dumps(message))

        def run(*args):
            # login_str = self.buildMySign()
            # ws.send(login_str)
            # operator('login', [self.api_key, self.passphrase, timestamp,sign]) # unix_time_seconds(datetime.now(timezone.utc))
            # time.sleep(5)
            # operator('subscribe', ['%s/position:%s' % (self.__market, self.__iid)])
            # operator('subscribe', ['%s/depth5:%s' % (self.__market, self.__iid)])
            operator('subscribe', ['%s/trade:%s' % (self.__market, self.__iid)])
            operator('subscribe', ['%s/mark_price:%s' % (self.__market, self.__iid)])
            operator('subscribe', ['%s/candle60s:%s' % (self.__market, self.__iid)])

            while True:
                ws.send("ping")
                time.sleep(30)

        threading.Thread(target=run).start()

    def sub(self):

        websocket.enableTrace(False)
        URL = "wss://real.okex.com:8443/ws/v3"
        ws = websocket.WebSocketApp(URL,
                                    on_message=self.incoming,
                                    on_error=self.error_handling,
                                    on_close=self.closing)

        ws.on_open = self.subscribe

        while True:
            try:
                ws.run_forever()
            except:
                pass

        pass

    def incoming(self,ws,message):
        message = zlib.decompress(message, -zlib.MAX_WBITS)
        message = message.decode('utf-8')
        global pong
        if 'pong' in message:
            pong = time.time()
        if 'trade' in message:
            d = json.loads(message)
            self.__last_trade_side = d['data'][0]['side']
            self.__last_trade_price = float(d['data'][0]['price'])
        if 'mark_price' in message:
            d = json.loads(message)
            self.__mark_price=float(d['data'][0]['mark_price'])
            self.__timestamp=pd.to_datetime(d['data'][0]['timestamp'])
        if 'candle' in message:
            d = json.loads(message)
            self.__candle = d['data'][0]['candle']
            if type(self.__candle)==list and len(self.__candle)>5:
                if self.__allcandle == []:
                    self.__allcandle.append(self.__candle)
                elif self.__allcandle[-1][0] == self.__candle[0]:
                    self.__allcandle[-1] = self.__candle
                else:
                    self.__allcandle.append(self.__candle)
                if len(self.__allcandle) > 5000:
                    with open(f"./data/live_candle_{self.__timestamp.strftime('%Y-%m-%dT%H:%M:%S')}.pkl", 'wb') as f:
                        pickle.dump(self.__allcandle, f)
                    self.__allcandle = self.__allcandle[-2880:]
#        if 'asks' in message and 'bids' in message:
#            d = json.loads(message)
#            self.__Depth = d['data'][0]
        # if 'long_qty' in message:
        #     d=json.loads(message)
        #     self.__long_open_outstanding=float(d['data'][0]['long_open_outstanding'])
        #     self.__short_open_outstanding=float(d['data'][0]['short_open_outstanding'])
        #     self.__positions=d['data']
        if self.__callbackEnabled:
            self.__callback(message)
    

    def error_handling(self,ws,error):
        print(str(error))

    def closing(self,ws):
        print("WebSocket Closing...")
        # sys.exit()
        os._exit(0)



