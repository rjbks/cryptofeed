'''
Copyright (C) 2017-2018  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json
import logging
import asyncio
from decimal import Decimal

import requests

from cryptofeed.feed import Feed
from cryptofeed.exchanges import BITMEX
from cryptofeed.defines import L2_BOOK, BID, ASK, TRADES


LOG = logging.getLogger('feedhandler')


class Bitmex(Feed):
    id = BITMEX
    api = 'https://www.bitmex.com/api/v1/'

    def __init__(self, *args, pairs=None, channels=None, callbacks=None, **kwargs):
        super().__init__('wss://www.bitmex.com/realtime',
                         *args,
                         pairs=None,
                         channels=channels,
                         callbacks=callbacks,
                         **kwargs)
        active_pairs = self.get_active_symbols()
        for pair in pairs:
            if pair not in active_pairs:
                raise ValueError("{} is not active on BitMEX".format(pair))
        self.pairs = pairs
        self.loop = asyncio.get_event_loop()

    async def _reset(self):
        self.partial_received = False
        self.order_id = {}
        for pair in self.pairs:
            await self.l2_book.delete_pair(pair)
            self.order_id[pair] = {}

    @staticmethod
    def get_symbol_info():
        return requests.get(Bitmex.api + 'instrument/').json()

    @staticmethod
    def get_active_symbols_info():
        return requests.get(Bitmex.api + 'instrument/active').json()
    
    @staticmethod
    def get_active_symbols():
        symbols = []
        for data in Bitmex.get_active_symbols_info():
            symbols.append(data['symbol'])
        return symbols

    async def _trade(self, msg):
        """
        trade msg example

        {
            'timestamp': '2018-05-19T12:25:26.632Z',
            'symbol': 'XBTUSD',
            'side': 'Buy',
            'size': 40,
            'price': 8335,
            'tickDirection': 'PlusTick',
            'trdMatchID': '5f4ecd49-f87f-41c0-06e3-4a9405b9cdde',
            'grossValue': 479920,
            'homeNotional': Decimal('0.0047992'),
            'foreignNotional': 40
        }
        """
        for data in msg['data']:
            await self.callbacks[TRADES](feed=self.id,
                                         pair=data['symbol'],
                                         side=BID if data['side'] == 'Buy' else ASK,
                                         amount=self.make_decimal(data['size']),
                                         price=self.make_decimal(data['price']),
                                         id=data['trdMatchID'])
    
    async def _book(self, msg):
        pair = None
        if not self.partial_received:
            # per bitmex documentation messages received before partial
            # should be discarded
            if msg['action'] != 'partial':
                return
            self.partial_received = True
        
        if msg['action'] == 'partial' or msg['action'] == 'insert':
            for data in msg['data']:
                side = BID if data['side'] == 'Buy' else ASK
                price = self.make_decimal(data['price'])
                pair = data['symbol']
                size = self.make_decimal(data['size'])
                await self.l2_book.set(pair, side, price, size)  # [pair][side][price] = size
                self.order_id[pair][data['id']] = (price, size)
        elif msg['action'] == 'update':
            for data in msg['data']:
                side = BID if data['side'] == 'Buy' else ASK
                pair = data['symbol']
                update_size = self.make_decimal(data['size'])
                price, _ = self.order_id[pair][data['id']]
                await self.l2_book.set(pair, side, price, update_size)  # [pair][side][price] = update_size
                self.order_id[pair][data['id']] = (price, update_size)
        elif msg['action'] == 'delete':
            for data in msg['data']:
                pair = data['symbol']
                side = BID if data['side'] == 'Buy' else ASK
                delete_price, delete_size = self.order_id[pair][data['id']]
                del self.order_id[pair][data['id']]
                # self.l2_book[pair][side][delete_price] -= delete_size
                # if await self.l2_book[pair][side][delete_price] == 0:
                #     await self.l2_book.remove_level(pair, side, delete_price)  # [pair][side][delete_price]
                await self.l2_book.decrement_and_remove_if_zero(pair, side, delete_price, delete_size)
        else:
            LOG.warning("{} - Unexpected L2 Book message {}".format(self.id, msg))
            return

        book = await self.l2_book.get_pair_book(pair)
        await self.callbacks[L2_BOOK](feed=self.id, pair=pair, book=book)

    async def message_handler(self, msg):
        msg = json.loads(msg, parse_float=Decimal)
        if 'info' in msg:
            LOG.info("%s - info message: %s", self.id, msg)
        elif 'subscribe' in msg:
            if not msg['success']:
                LOG.error("{} - subscribe failed: {}".format(self.id, msg))
        elif 'error' in msg:
            LOG.error("{} - Error message from exchange: {}".format(self.id, msg))
        else:
            if msg['table'] == 'trade':
                await self._trade(msg)
            elif msg['table'] == 'orderBookL2':
                await self._book(msg)
            else:
                LOG.warning("{} - Unhandled message {}".format(self.id, msg))

    async def subscribe(self, websocket):
        await self._reset()
        chans = []
        for channel in self.channels:
            for pair in self.pairs:
                chans.append("{}:{}".format(channel, pair))
    
        await websocket.send(json.dumps({"op": "subscribe", 
                                         "args": chans}))
