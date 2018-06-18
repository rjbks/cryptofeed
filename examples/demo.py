'''
Copyright (C) 2017-2018  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed.callback import TickerCallback, TradeCallback, BookCallback, L3BookCallback, L3BookUpdateCallback
from cryptofeed import FeedHandler
from cryptofeed import Bitmex, GDAX, Bitfinex, Poloniex, Gemini, HitBTC, Bitstamp
from cryptofeed.defines import L3_BOOK, L3_BOOK_UPDATE, L2_BOOK, BID, ASK, TRADES, TICKER
from cryptofeed.order_books import RedisOrderBook


# Examples of some handlers for different updates. These currently don't do much.
# Handlers should conform to the patterns/signatures in callback.py
# Handlers can be normal methods/functions or async. The feedhandler is paused
# while the callbacks are being handled (unless they in turn await other functions or I/O)
# so they should be as lightweight as possible
async def ticker(feed, pair, bid, ask):
    print('Feed: {} Pair: {} Bid: {} Ask: {}'.format(feed, pair, bid, ask))


async def trade(feed, pair, id, timestamp, side, amount, price):
        print("Timestamp: {} Feed: {} Pair: {} ID: {} Side: {} Amount: {} Price: {}".format(
            timestamp,
            feed,
            pair,
            id,
            side,
            amount,
            price
        ))


async def l3book(feed, timestamp, sequence, pair, book):
        print('Feed: {} Timestamp: {} Sequence: {} Pair: {} Book Bid Size is {} Ask Size is {}'.format(
            feed,
            timestamp,
            sequence,
            pair,
            len(book[BID]),
            len(book[ASK])
        ))


async def book(feed, pair, book):
    print('Feed: {} Pair: {} Book Bid Size is {} Ask Size is {}'.format(feed, pair, len(book[BID]), len(book[ASK])))


async def l3bookupdate(feed, pair, msg_type, timestamp, sequence, side, price, size):
    print('Feed: {} Pair: {} Message Type: {} Timestamp: {} Sequence: {} Side: {} Price: {} Size: {}'.format(
        feed,
        pair,
        msg_type,
        timestamp,
        sequence,
        side,
        price,
        size
    ))


def main():
    f = FeedHandler()
    # f.add_feed(GDAX(
    #     pairs=['BTC-USD'],
    #     channels=[TICKER, TRADES],
    #     callbacks={TICKER: TickerCallback(ticker),
    #                TRADES: TradeCallback(trade)}
    # ))
    f.add_feed(GDAX(
        pairs=['BTC-USD'],
        channels=[L3_BOOK_UPDATE, L3_BOOK],
        callbacks={L3_BOOK: L3BookCallback(l3book),
                   L3_BOOK_UPDATE: L3BookUpdateCallback(l3bookupdate)},
        intervals={'_book_snapshot': 3},
        order_book_cls=RedisOrderBook,
    ))
    # f.add_feed(Bitfinex(
    #     pairs=['BTC-USD'],
    #     channels=[L3_BOOK],
    #     callbacks={L3_BOOK: L3BookCallback(l3book)}
    # ))
    # f.add_feed(Poloniex(
    #     channels=[TICKER, 'USDT-BTC'],
    #     callbacks={L3_BOOK_UPDATE: L3BookUpdateCallback(l3bookupdate),
    #                L3_BOOK: L3BookCallback(l3book),
    #                TICKER: TickerCallback(ticker)}
    # ))
    # f.add_feed(Gemini(
    #     pairs=['BTC-USD'],
    #     channels=[L3_BOOK],
    #     callbacks={L3_BOOK_UPDATE: L3BookUpdateCallback(l3bookupdate),
    #                L3_BOOK: L3BookCallback(l3book),
    #                TRADES: TradeCallback(trade)},
    #     intervals={'_book_snapshot': 3}
    # ))
    # f.add_feed(HitBTC(
    #     channels=[TRADES],
    #     pairs=['BTC-USD'],
    #     callbacks={TRADES: TradeCallback(trade)}
    # ))
    # f.add_feed(Bitstamp(
    #     channels=[L3_BOOK, TRADES],
    #     pairs=['BTC-USD'],
    #     callbacks={L3_BOOK: L3BookCallback(l3book),
    #                TRADES: TradeCallback(trade)}
    # ))
    #
    # bitmex_symbols = Bitmex.get_active_symbols()
    # f.add_feed(Bitmex(
    #     channels=[TRADES],
    #     pairs=bitmex_symbols,
    #     callbacks={TRADES: TradeCallback(trade)}
    # ))
    # f.add_feed(Bitmex(
    #     pairs=['XBTUSD'],
    #     channels=[L2_BOOK],
    #     callbacks={L2_BOOK: BookCallback(book)}
    # ))
    f.run()

# async def ticker(feed, pair, bid, ask):
#     print('Feed: {} Pair: {} Bid: {} Ask: {}'.format(feed, pair, bid, ask))
#
#
# async def trade(feed, pair, id, timestamp, side, amount, price):
#         print("Timestamp: {} Feed: {} Pair: {} ID: {} Side: {} Amount: {} Price: {}".format(
#             timestamp,
#             feed,
#             pair,
#             id,
#             side,
#             amount,
#             price
#         ))
#
#
# async def l3book(feed, timestamp, sequence, pair, book):
#         print('Feed: {} Timestamp: {} Sequence: {} Pair: {} Book Bid Size is {} Ask Size is {}'.format(
#             feed,
#             timestamp,
#             sequence,
#             pair,
#             len(book[BID]),
#             len(book[ASK])
#         ))
#
#
# async def book(feed, pair, book):
#     print('Feed: {} Pair: {} Book Bid Size is {} Ask Size is {}'.format(feed, pair, len(book[BID]), len(book[ASK])))
#
#
# async def l3bookupdate(feed, pair, msg_type, timestamp, sequence, side, price, size):
#     print('Feed: {} Pair: {} Message Type: {} Timestamp: {} Sequence: {} Side: {} Price: {} Size: {}'.format(
#         feed,
#         pair,
#         msg_type,
#         timestamp,
#         sequence,
#         side,
#         price,
#         size
#     ))
#
#
# def main():
#     f = FeedHandler()
#     f.add_feed(GDAX(
#         pairs=['BTC-USD'],
#         channels=[TICKER, TRADES],
#         callbacks={TICKER: TickerCallback(ticker),
#                    TRADES: TradeCallback(trade)}
#     ))
#     f.add_feed(GDAX(
#         pairs=['BTC-USD'],
#         channels=[L3_BOOK_UPDATE, L3_BOOK],
#         callbacks={L3_BOOK: L3BookCallback(l3book),
#                    L3_BOOK_UPDATE: L3BookUpdateCallback(l3bookupdate)},
#         intervals={'_book_snapshot': 3}
#     ))
#     f.add_feed(Bitfinex(
#         pairs=['BTC-USD'],
#         channels=[L3_BOOK],
#         callbacks={L3_BOOK: L3BookCallback(l3book)}
#     ))
#     f.add_feed(Poloniex(
#         channels=[TICKER, 'USDT-BTC'],
#         callbacks={L3_BOOK_UPDATE: L3BookUpdateCallback(l3bookupdate),
#                    L3_BOOK: L3BookCallback(l3book),
#                    TICKER: TickerCallback(ticker)}
#     ))
#     f.add_feed(Gemini(
#         pairs=['BTC-USD'],
#         channels=[L3_BOOK],
#         callbacks={L3_BOOK_UPDATE: L3BookUpdateCallback(l3bookupdate),
#                    L3_BOOK: L3BookCallback(l3book),
#                    TRADES: TradeCallback(trade)},
#         intervals={'_book_snapshot': 3}
#     ))
#     f.add_feed(HitBTC(
#         channels=[TRADES],
#         pairs=['BTC-USD'],
#         callbacks={TRADES: TradeCallback(trade)}
#     ))
#     f.add_feed(Bitstamp(
#         channels=[L3_BOOK, TRADES],
#         pairs=['BTC-USD'],
#         callbacks={L3_BOOK: L3BookCallback(l3book),
#                    TRADES: TradeCallback(trade)}
#     ))
#
#     bitmex_symbols = Bitmex.get_active_symbols()
#     f.add_feed(Bitmex(
#         channels=[TRADES],
#         pairs=bitmex_symbols,
#         callbacks={TRADES: TradeCallback(trade)}
#     ))
#     f.add_feed(Bitmex(
#         pairs=['XBTUSD'],
#         channels=[L2_BOOK],
#         callbacks={L2_BOOK: BookCallback(book)}
#     ))
#     f.run()


if __name__ == '__main__':
    main()
