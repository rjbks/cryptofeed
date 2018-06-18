'''
Copyright (C) 2017-2018  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
import json
import logging
from decimal import Decimal

import requests

from cryptofeed.feed import Feed
from cryptofeed.exchanges import GDAX as GDAX_ID
from cryptofeed.defines import L2_BOOK, L3_BOOK, L3_BOOK_UPDATE, BID, ASK, TRADES, TICKER


LOG = logging.getLogger('feedhandler')


class GDAX(Feed):
    id = GDAX_ID

    def __init__(self, *args, pairs=None, channels=None, callbacks=None, **kwargs):
        super().__init__('wss://ws-feed.gdax.com', *args, pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)
        self.order_map = {}
        self.seq_no = {}
        self.book = self.l3_book
        # self._precision = None

    # @property
    # def precision(self):
    #     if self._precision is None:
    #         url = 'https://api.gdax.com/products'
    #         result = requests.get(url).json()
    #         self._precision = {
    #             pair: {
    #                 'base': len(data['base_min_size'].split('.')[-1]),
    #                 'quoted': len(data['quote_increment'].split('.')[-1])
    #             }
    #             for pair, data in result.items()
    #         }
    #     return self._precision

    async def _ticker(self, msg):
        '''
        {
            'type': 'ticker',
            'sequence': 5928281084,
            'product_id': 'BTC-USD',
            'price': '8500.01000000',
            'open_24h': '8217.24000000',
            'volume_24h': '4529.1293778',
            'low_24h': '8172.00000000',
            'high_24h': '8600.00000000',
            'volume_30d': '329178.93594133',
            'best_bid': '8500',
            'best_ask': '8500.01'
        }

        {
            'type': 'ticker',
            'sequence': 5928281348,
            'product_id': 'BTC-USD',
            'price': '8500.00000000',
            'open_24h': '8217.24000000',
            'volume_24h': '4529.13179472',
            'low_24h': '8172.00000000',
            'high_24h': '8600.00000000',
            'volume_30d': '329178.93835825',
            'best_bid': '8500',
            'best_ask': '8500.01',
            'side': 'sell',
            'time': '2018-05-21T00:30:11.587000Z',
            'trade_id': 43736677,
            'last_size': '0.00241692'
        }
        '''
        await self.callbacks[TICKER](feed=self.id,
                                     pair=msg['product_id'],
                                     bid=self.make_decimal(msg['best_bid']),
                                     ask=self.make_decimal(msg['best_ask']))

    async def _book_update(self, msg):
        '''
        {
            'type': 'match', or last_match
            'trade_id': 43736593
            'maker_order_id': '2663b65f-b74e-4513-909d-975e3910cf22',
            'taker_order_id': 'd058d737-87f1-4763-bbb4-c2ccf2a40bde',
            'side': 'buy',
            'size': '0.01235647',
            'price': '8506.26000000',
            'product_id': 'BTC-USD',
            'sequence': 5928276661,
            'time': '2018-05-21T00:26:05.585000Z'
        }
        '''
        sequence = msg['sequence']
        timestamp = self.tz_aware_datetime_from_string(msg['time'])
        pair = msg['product_id']
        price = self.make_decimal(msg['price'])
        side = ASK if msg['side'] == 'sell' else BID
        size = self.make_decimal(msg['size'])
        if self.book:
            maker_order_id = msg['maker_order_id']

            self.order_map[maker_order_id]['size'] -= size
            if self.order_map[maker_order_id]['size'] <= 0:
                del self.order_map[maker_order_id]

            # self.book[pair][side][price] -= size
            # if self.book[pair][side][price] == 0:
            #     del self.book[pair][side][price]
            await self.book.decrement_and_remove_if_zero(pair, side, price, size)

            await self.callbacks[L3_BOOK_UPDATE](
                feed=self.id,
                pair=pair,
                msg_type='trade',
                timestamp=timestamp,
                sequence=sequence,
                side=side,
                price=price,
                size=size
            )

        await self.callbacks[TRADES](
                feed=self.id,
                pair=pair,
                id=msg['trade_id'],
                side=side,
                amount=size,
                price=price,
                timestamp=timestamp
            )

    async def _pair_level2_snapshot(self, msg):
        pair = msg['product_id']
        book = {
                BID: {
                    price: amount
                    for price, amount in msg['bids']
                },
                ASK: {
                    price: amount
                    for price, amount in msg['asks']
                }
            }
        await self.l2_book.set_pair_book(pair, book)

    async def _pair_level2_update(self, msg):
        pair = msg['product_id']
        for side, price, amount in msg['changes']:
            side = BID if side == 'buy' else ASK
            price = self.make_decimal(price)
            amount = self.make_decimal(amount)
            # bidask = self.l2_book[pair][BID if side == 'buy' else ASK]

            if amount == 0:
                # if price in bidask:
                #     del bidask[price]
                await self.l2_book.remove_if_exists(pair, side, price)
            else:
                # bidask[price] = amount
                await self.l2_book.set(pair, side, price, amount)
        book = self.l2_book.get_pair_book(pair)
        await self.callbacks[L2_BOOK](feed=self.id, pair=pair, book=book)

    async def _book_snapshot(self, pair, update_book=True, ignore_sequence=False):
        loop = asyncio.get_event_loop()
        url = 'https://api.gdax.com/products/{}/book?level=3'.format(pair)
        result = await loop.run_in_executor(None, requests.get, url)
        orders = result.json()
        seq_no = orders['sequence']
        book = {BID: {}, ASK: {}}

        for side in (BID, ASK):
            book_side = book[side]
            for price, size, order_id in orders[side + 's']:
                price = self.make_decimal(price)
                size = self.make_decimal(size)

                if price in book_side:
                    book_side[price] += size
                else:
                    book_side[price] = size

                if update_book:
                    self.order_map[order_id] = {'price': price, 'size': size}

        if update_book:
            await self.book.set_pair_book(pair, book)

        if not ignore_sequence:
            self.seq_no[pair] = seq_no

        await self.callbacks[L3_BOOK](feed=self.id,
                                      pair=pair,
                                      timestamp=None,
                                      sequence=seq_no,
                                      book=book)

    async def _open(self, msg):
        price = self.make_decimal(msg['price'])
        side = ASK if msg['side'] == 'sell' else BID
        size = self.make_decimal(msg['remaining_size'])
        pair = msg['product_id']
        order_id = msg['order_id']
        sequence = msg['sequence']
        timestamp = self.tz_aware_datetime_from_string(msg['time'])

        # if price in self.book[pair][side]:
        #     self.book[pair][side][price] += size
        # else:
        #     self.book[pair][side][price] = size
        await self.book.increment_if_exists_else_set_abs(pair, side, price, size)

        self.order_map[order_id] = {'price': price, 'size': size}
        await self.callbacks[L3_BOOK_UPDATE](
                feed=self.id,
                pair=pair,
                msg_type='open',
                timestamp=timestamp,
                sequence=sequence,
                side=side,
                price=price,
                size=size
        )

    async def _done(self, msg):
        if 'price' not in msg:
            return
        order_id = msg['order_id']
        if order_id not in self.order_map:
            return
        price = self.make_decimal(msg['price'])
        side = ASK if msg['side'] == 'sell' else BID
        pair = msg['product_id']
        size = self.order_map[order_id]['size']
        sequence = msg['sequence']
        timestamp = self.tz_aware_datetime_from_string(msg['time'])

        # if self.book[pair][side][price] - size == 0:
        #     del self.book[pair][side][price]
        # else:
        #     self.book[pair][side][price] -= size

        await self.book.decrement_and_remove_if_zero(pair, side, price, size)

        del self.order_map[order_id]
        await self.callbacks[L3_BOOK_UPDATE](
                feed=self.id,
                pair=pair,
                msg_type='done',
                timestamp=timestamp,
                sequence=sequence,
                side=side,
                price=price,
                size=size
            )

    async def _change(self, msg):
        order_id = msg['order_id']
        if order_id not in self.order_map:
            return
        price = self.make_decimal(msg['price'])
        side = ASK if msg['side'] == 'sell' else BID
        new_size = self.make_decimal(msg['new_size'])
        old_size = self.make_decimal(msg['old_size'])
        pair = msg['product_id']

        size = old_size - new_size
        sequence = msg['sequence']
        timestamp = self.tz_aware_datetime_from_string(msg['time'])
        # self.book[pair][side][price] -= size
        await self.book.increment(pair, side, price, -size)
        self.order_map[order_id] = new_size

        await self.callbacks[L3_BOOK_UPDATE](
                feed=self.id,
                pair=pair,
                msg_type='change',
                timestamp=timestamp,
                sequence=sequence,
                side=side,
                price=price,
                size=size
            )

    async def message_handler(self, msg: str):
        msg = json.loads(msg, parse_float=Decimal)
        if 'full' in self.channels and 'product_id' in msg and 'sequence' in msg:
            pair = msg['product_id']
            if pair not in self.seq_no:
                self.seq_no[pair] = msg['sequence']
            elif msg['sequence'] <= self.seq_no[pair]:
                return
            elif 'full' in self.channels and msg['sequence'] != self.seq_no[pair] + 1:
                LOG.warning("Missing sequence number detected")
                LOG.warning("Requesting book snapshot")
                await self._book_snapshot(pair)
                return
        
            self.seq_no[pair] = msg['sequence']

        if 'type' in msg:
            if msg['type'] == 'ticker':
                await self._ticker(msg)
            elif msg['type'] == 'match' or msg['type'] == 'last_match':
                await self._book_update(msg)
            elif msg['type'] == 'snapshot':
                await self._pair_level2_snapshot(msg)
            elif msg['type'] == 'l2update':
                await self._pair_level2_update(msg)
            elif msg['type'] == 'open':
                await self._open(msg)
            elif msg['type'] == 'done':
                await self._done(msg)
            elif msg['type'] == 'change':
                await self._change(msg)
            elif msg['type'] == 'received':
                pass
            elif msg['type'] == 'activate':
                pass
            elif msg['type'] == 'subscriptions':
                pass
            else:
                LOG.warning('{} - Invalid message type {}'.format(self.id, msg))

    async def subscribe(self, websocket):
        l3_book = False
        # remove l3_book from channels as we will be synthesizing that feed
        if L3_BOOK in self.channels:
            l3_book = True
            self.channels.pop(self.channels.index(L3_BOOK))

        await websocket.send(json.dumps({"type": "subscribe",
                                         "product_ids": self.pairs,
                                         "channels": self.channels
                                        }))
        if l3_book:
            for pair in self.pairs:
                asyncio.ensure_future(self.synthesize_feed(self._book_snapshot,
                                                           pair,
                                                           update_book=False,
                                                           ignore_sequence=True))
        if 'full' in self.channels:
            await asyncio.gather(*[self._book_snapshot(pair) for pair in self.pairs])
