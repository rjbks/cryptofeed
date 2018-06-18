'''
Copyright (C) 2017-2018  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json
import logging
from decimal import Decimal

from cryptofeed.feed import Feed
from cryptofeed.defines import BID, ASK, TRADES, TICKER, L3_BOOK, L3_BOOK_UPDATE, VOLUME
from cryptofeed.standards import pair_std_to_exchange, pair_exchange_to_std
from cryptofeed.exchanges import POLONIEX
from .pairs import poloniex_id_pair_mapping


LOG = logging.getLogger('feedhandler')


class Poloniex(Feed):
    id = POLONIEX

    def __init__(self, *args, pairs=None, channels=None, callbacks=None, **kwargs):
        if pairs:
            LOG.error("Poloniex does not support pairs")
            raise ValueError("Poloniex does not support pairs")

        super().__init__('wss://api2.poloniex.com',
                         *args,
                         channels=channels,
                         callbacks=callbacks,
                         **kwargs)

    async def _ticker(self, msg):
        # currencyPair, last, lowestAsk, highestBid, percentChange, baseVolume,
        # quoteVolume, isFrozen, 24hrHigh, 24hrLow
        pair_id, _, ask, bid, _, _, _, _, _, _ = msg
        pair = pair_exchange_to_std(poloniex_id_pair_mapping[pair_id])
        await self.callbacks[TICKER](feed=self.id,
                                     pair=pair,
                                     bid=Decimal(bid),
                                     ask=Decimal(ask))

    async def _volume(self, msg):
        # ['2018-01-02 00:45', 35361, {'BTC': '43811.201', 'ETH': '6747.243', 'XMR': '781.716', 'USDT': '196758644.806'}]
        # timestamp, exchange volume, dict of top volumes
        _, _, top_vols = msg
        for pair in top_vols:
            top_vols[pair] = Decimal(top_vols[pair])
        self.callbacks[VOLUME](feed=self.id, **top_vols)

    async def _book(self, msg, chan_id, sequence):
        msg_type = msg[0][0]
        pair = None
        # initial update (i.e. snapshot)
        if msg_type == 'i':
            pair = msg[0][1]['currencyPair']
            pair = pair_exchange_to_std(pair)
            await self.l3_book.delete_pair(pair)  # self.l3_book[pair] = {BID: sd(), ASK: sd()}
            # 0 is asks, 1 is bids
            order_book = msg[0][1]['orderBook']
            for key in order_book[0]:
                amount = Decimal(order_book[0][key])
                price = Decimal(key)
                await self.l3_book.set(pair, ASK, price, amount)  # [pair][ASK][price] = amount

            for key in order_book[1]:
                amount = Decimal(order_book[1][key])
                price = Decimal(key)
                await self.l3_book.set(pair, BID, price, amount)  # [pair][BID][price] = amount
        else:
            pair = poloniex_id_pair_mapping[chan_id]
            pair = pair_exchange_to_std(pair)
            for update in msg:
                timestamp = None
                msg_type = update[0]
                # order book update
                if msg_type == 'o':
                    mtype = 'change'
                    side = ASK if update[1] == 0 else BID
                    price = Decimal(update[2])
                    amount = Decimal(update[3])
                    if amount == 0:
                        await self.l3_book.remove(pair, side, price)  # [pair][side][price]
                    else:
                        await self.l3_book.set(pair, side, price, amount)  # [pair][side][price] = amount
                elif msg_type == 't':
                    # index 1 is trade id, 2 is side, 3 is price, 4 is amount, 5 is timestamp
                    mtype = 'trade'
                    timestamp = self.tz_aware_datetime_from_string(update[5])
                    price = Decimal(update[3])
                    side = ASK if update[2] == 0 else BID
                    amount = Decimal(update[4])
                    await self.callbacks[TRADES](feed=self.id,
                                                 pair=pair,
                                                 side=side,
                                                 amount=amount,
                                                 price=price)
                else:
                    LOG.warning("{} - Unexpected message received: {}".format(self.id, msg))
                    # continue so we don't hit the callback below if the msg_type isn't of the 2 tested for above
                    continue

                await self.callbacks[L3_BOOK_UPDATE](feed=self.id,
                                                     pair=pair,
                                                     msg_type=mtype,
                                                     timestamp=timestamp,
                                                     sequence=sequence,
                                                     side=side,
                                                     price=price,
                                                     size=amount)

        book = await self.l3_book.get_pair_book(pair)
        await self.callbacks[L3_BOOK](feed=self.id,
                                      sequence=sequence,
                                      timestamp=None,
                                      pair=pair,
                                      book=book)

    async def message_handler(self, msg):
        msg = json.loads(msg, parse_float=Decimal)
        if 'error' in msg:
            LOG.error("{} - Error from exchange: {}".format(self.id, msg))
            return

        chan_id = msg[0]
        if chan_id == 1002:
            # the ticker channel doesn't have sequence ids
            # so it should be None, except for the subscription
            # ack, in which case its 1
            seq_id = msg[1]
            if seq_id is None:
                await self._ticker(msg[2])
        elif chan_id == 1003:
            # volume update channel is just like ticker - the
            # sequence id is None except for the initial ack
            seq_id = msg[1]
            if seq_id is None:
                await self._volume(msg[2])
        elif chan_id <= 200:
            # order book updates - the channel id refers to
            # the trading pair being updated
            seq_id = msg[1]
            await self._book(msg[2], chan_id, seq_id)
        elif chan_id == 1010:
            # heartbeat - ignore
            pass
        else:
            LOG.warning('{} - Invalid message type {}'.format(self.id, msg))

    async def subscribe(self, websocket):
        for channel in self.channels:
            await websocket.send(json.dumps({"command": "subscribe",
                                             "channel": channel
                                            }))
