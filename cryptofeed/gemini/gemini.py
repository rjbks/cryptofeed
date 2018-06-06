'''
Copyright (C) 2017-2018  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json
import logging
from decimal import Decimal

from sortedcontainers import SortedDict as sd

from cryptofeed.feed import Feed
from cryptofeed.exchanges import GEMINI
from cryptofeed.defines import L3_BOOK, L3_BOOK_UPDATE, BID, ASK, TRADES
from cryptofeed.standards import pair_std_to_exchange


LOG = logging.getLogger('feedhandler')


class Gemini(Feed):
    id = GEMINI

    def __init__(self, pairs=None, channels=None, callbacks=None):
        if len(pairs) != 1:
            LOG.error("Gemini requires a websocket per trading pair")
            raise ValueError("Gemini requires a websocket per trading pair")
        if channels is not None:
            LOG.error("Gemini does not support different channels")
            raise ValueError("Gemini does not support different channels")
        self.pair = pairs[0]

        super().__init__('wss://api.gemini.com/v1/marketdata/' + pair_std_to_exchange(self.pair, 'GEMINI'),
                         pairs=None,
                         channels=None,
                         callbacks=callbacks)
        self.book = {BID: sd(), ASK: sd()}

    async def _book(self, msg):
        sequence = msg['sequence']
        timestamp = msg['timestamp']
        side = BID if msg['side'] == 'bid' else ASK
        price = Decimal(msg['price'])
        remaining = Decimal(msg['remaining'])
        # delta = Decimal(msg['delta'])

        if msg['reason'] == 'initial':
            self.book[side][price] = remaining
        else:
            if remaining == 0:
                del self.book[side][price]
            else:
                self.book[side][price] = remaining
        await self.callbacks[L3_BOOK](feed=self.id, sequence=sequence, timestamp=timestamp,
                                      pair=self.pair, book=self.book)

    async def _trade(self, msg):
        price = Decimal(msg['price'])
        side = BID if msg['makerSide'] == 'bid' else ASK
        amount = Decimal(msg['amount'])
        await self.callbacks[TRADES](feed=self.id, id=msg['eventId'], pair=self.pair, side=side, amount=amount, price=price)

    async def _update(self, msg):
        sequence = msg['socket_sequence']
        if sequence is not 0:
            timestamp = (
                Decimal(msg['timestampms']) / Decimal(1000)
            ) if msg.get('timestampms') else Decimal(msg['timestamp'])
        else:
            timestamp = None
        for update in msg['events']:
            update['timestamp'] = timestamp
            update['sequence'] = sequence
            if update['type'] == 'change':
                await self._book(update)
            elif update['type'] == 'trade':
                await self._trade(update)
            elif update['type'] == 'auction':
                pass
            elif update['type'] == 'block_trade':
                pass
            else:
                LOG.warning("Invalid update received {}".format(update))

    async def message_handler(self, msg):
        msg = json.loads(msg, parse_float=Decimal)
        if msg['type'] == 'update':
            await self._update(msg)
        elif msg['type'] == 'heartbeat':
            pass
        else:
            LOG.warning('Invalid message type {}'.format(msg))

    async def subscribe(self, *args):
        return
