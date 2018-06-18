import asyncio
from decimal import Decimal

from sortedcontainers import SortedDict as sd

from cryptofeed.defines import BID, ASK
from cryptofeed.order_books.base_order_book import OrderBookBase
from cryptofeed.order_books.redis.pool import RedisPool


class RedisOrderBook(OrderBookBase):
    """
    Redis backed order book store for an exchange
    For each price/size pair stored in the hashmap, the price is also stored in
    a sorted set
    Naming convention:
        '{Exchange}:{pair}:(bid|ask)' -> hashmap of price, size pairs
        '{Exchange}:{pair}:(bid|ask):prices' -> sorted set of prices
    """

    def __init__(self, exchange, *args, **kwargs):
        super().__init__(exchange)
        self._book = {}
        self.pool = RedisPool()

    def make_key(self, pair: str, side: str, prices: bool=False)-> str:
        """
        creates key string for redis book resources
        :param pair: str
        :param side: str
        :param prices: bool-> True to make a key for prices sorted set
        :return: str-> redis key string for book resource
        """
        key = '{}:{}'.format(self.exchange, pair)
        if side:
            key = '{}:{}'.format(key, side)
            if prices:
                key = '{}:prices'.format(key)
        return key

    async def get_pairs(self)-> list:
        """
        get all pairs in exchange book
        :return: list-> list of all pairs in book
        """
        pattern = '{}:*:bid'.format(self.exchange)
        return list(set([
            key.split(':')[1]  # second element is the pair symbol
            async for key in self.pool.iscan(match=pattern)
        ]))

    async def set_pair_book(self, pair: str, book: dict)-> None:
        """
        set BID/ASK books for specified pair
        deletes all books and corresponding sorted sets for
        the pair in this exchange prior to setting new values
        :param pair: str
        :param book: dict
        :return: None
        """
        bidkey = self.make_key(pair, side=BID)
        bidpriceskey = self.make_key(pair, side=BID, prices=True)  # bid prices sorted set
        askkey = self.make_key(pair, side=ASK)
        askpriceskey = self.make_key(pair, side=ASK, prices=True)  # ask prices sorted set

        bids = {}
        asks = {}

        bid_prices = []
        for price, size in book.get(BID, {}).items():
            price = str(Decimal(price).normalize())  # Decimal conversion normalizes precision across price keys
            bid_prices.extend([float(price), price])
            bids[price] = str(size)

        ask_prices = []
        for price, size in book.get(ASK, {}).items():
            price = str(Decimal(price).normalize())
            ask_prices.extend([float(price), price])
            asks[price] = str(size)

        transaction = self.pool.multi_exec()
        transaction.delete(bidkey,
                           askkey,
                           bidpriceskey,
                           askpriceskey)
        if bids:
            transaction.hmset_dict(bidkey, bids)
            transaction.zadd(bidpriceskey, *bid_prices)
        if asks:
            transaction.hmset_dict(askkey, asks)
            transaction.zadd(askpriceskey, *ask_prices)
        await transaction.execute()

    async def get_exchange_book(self)-> dict:
        """
        get all BID/ASK books for each pair in the
        :return: dict all order books for this exchange:
            ({pair: {BID: sd({p1:s1,...}), ASK: sd({...}), ...})
        """
        # glob pattern for redis scan
        book_pattern = '{}:*:[ba][is][dk]'.format(self.exchange)
        seen = []
        futures = []
        async for key in self.pool.iscan(match=book_pattern):
            _, pair, _ = key.split(':')
            if pair not in seen:
                seen.append(pair)
                futures.append(self.get_pair_book(pair))
        return {
            pair: book
            for pair, book in zip(seen, await asyncio.gather(*futures))
        }

    async def get_pair_book(self, pair: str)-> dict:
        """
        get BID/ASK books for specified pair
        :param pair: str
        :return: tuple-> (pair, dict of BID/ASK SortedDicts
            {BID: sd({p1:s1,...}), ASK: sd({...})})
        """
        bidkey = self.make_key(pair, side=BID)
        askkey = self.make_key(pair, side=ASK)
        bids, asks = await asyncio.gather(self.pool.hgetall(bidkey),
                                          self.pool.hgetall(askkey))
        return {
            BID: sd({
                Decimal(price): Decimal(size)
                for price, size in bids.items()
            }),
            ASK: sd({
                Decimal(price): Decimal(size)
                for price, size in asks.items()
            })
        }

    async def get_pair_side(self, pair: str, side: str)-> sd:
        """
        get BID or ASK side of book for specified pair
        :param pair: str
        :param side: str
        :return: SortedDict-> price/size pairs for specified side of pair
        """
        key = self.make_key(pair, side=side)
        book_side = await self.pool.hgetall(key)
        return sd({
            Decimal(price): Decimal(size)
            for price, size in book_side.items()
        })

    async def sorted_prices_for_pair(self, pair: str)-> dict:
        """
        Gets dict (with keys "bid"/"ask") of sorted price lists
        (ascending for asks, descending for bids)
        fo each side
        :param pair: str
        :return: dict -> {BID: [100, 99], ASK: [101, 102]}
        """
        bidkey = self.make_key(pair, side=BID, prices=True)
        askkey = self.make_key(pair, side=ASK, prices=True)
        bids, asks = await asyncio.gather(self.pool.zrevrange(bidkey, 0, -1),
                                          self.pool.zrange(askkey, 0, -1))
        return {BID: bids, ASK: asks}

    async def sorted_bids_for_pair(self, pair: str)-> list:
        """
        gets a sorted list of all bids (descending)
        :param pair: str
        :return: list -> descending bids
        """
        key = self.make_key(pair, side=BID, prices=True)
        return [Decimal(price) for price in await self.pool.zrevrange(key, 0, -1)]

    async def sorted_asks_for_pair(self, pair: str)-> list:
        """
        gets a sorted list of all asks (ascending)
        :param pair: str
        :return: list -> ascending asks
        """
        key = self.make_key(pair, side=ASK, prices=True)
        return [Decimal(price) for price in await self.pool.zrange(key, 0, -1)]

    async def delete_pair(self, pair: str)-> None:
        """
        Removes pair from book
        :param pair: str
        :return: None
        """
        bidkey = self.make_key(pair, side=BID)
        bidpriceskey = self.make_key(pair, side=BID, prices=True)  # bid prices sorted set
        askkey = self.make_key(pair, side=ASK)
        askpriceskey = self.make_key(pair, side=ASK, prices=True)  # ask prices sorted set
        await self.pool.delete(bidkey,
                               askkey,
                               bidpriceskey,
                               askpriceskey)

    async def price_exists(self, pair: str, side: str, price: Decimal)-> bool:
        """
        Checks if price exists in specified book
        :param pair: str
        :param side: str
        :param price: str
        :return: bool
        """
        key = self.make_key(pair, side=side)
        return await self.pool.hexists(key, str(price))

    async def get(self, pair: str, side: str, price: Decimal, default=None):
        """
        Fetch size at level from specified book, returns `default` if not found.
        :param pair: str
        :param side: str ('bid', 'ask')
        :param price: decimal/str/float
        :param default: decimal/str/float/None
        :return: Decimal/None -> size value of level or None
        """
        key = self.make_key(pair, side=side)
        val = await self.pool.hget(key, str(price))
        return Decimal(val) if val is not None else default

    async def set(self, pair: str, side: str, price: Decimal, size)-> None:
        """
        Sets the price level to the specified size
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal
        :return: None
        """
        price = str(price)
        hkey = self.make_key(pair, side=side)
        zkey = self.make_key(pair, side=side, prices=True)
        transaction = self.pool.multi_exec()
        transaction.hset(hkey, price, str(size))
        transaction.zadd(zkey, float(price), price)
        await transaction.execute()

    async def remove(self, pair: str, side: str, price: Decimal)-> None:
        """
        Remove the specified price level from the book and sorted set
        :param pair: str
        :param side: str
        :param price: decimal.Decimal
        :return: None
        """
        price = str(price)
        hkey = self.make_key(pair, side=side)
        zkey = self.make_key(pair, side=side, prices=True)
        transaction = self.pool.multi_exec()
        transaction.hdel(hkey, price)
        transaction.zrem(zkey, price)
        await transaction.execute()

    async def remove_if_zero_size(self, pair: str, side: str, price: Decimal)-> bool:
        """
        remove price from book if size is 0
        :param pair: str
        :param side: str
        :param price: decimal.Decimal
        :return: bool-> removed
        """
        key = self.make_key(pair, side=side)
        return await self.pool.evalsha(
            self.pool.delete_if_zero_size,
            keys=[key],
            args=[str(price)]
        ) is not None

    async def remove_if_exists(self, pair: str, side: str, price: Decimal):
        """
        remove price level if it exists
        redis doesn't throw an error when deleting non-existant fields
        :param pair: str
        :param side: str
        :param price: decimal.Decimal
        :return: None
        """
        await self.remove(pair, side, price)

    async def increment(self, pair: str, side: str, price: Decimal, size)-> None:
        """
        Increment the size of the specified price level
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal-> adds this amount to book
        :return: None
        """
        key = self.make_key(pair, side=side)
        await self.pool.hincrbyfloat(key, str(price), float(size))

    async def increment_if_exists(self, pair: str, side: str, price: Decimal, size)-> bool:
        """
        Increment the size of the specified price level if it exists
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal-> adds this amount to book
        :return: bool-> price exists
        """
        key = self.make_key(pair, side=side)
        return await self.pool.evalsha(
            self.pool.incr_if_exists,
            keys=[key],
            args=[str(price), str(size)]
        ) is not None

    async def increment_if_exists_else_set_abs(self, pair: str, side: str, price: Decimal, size)-> bool:
        """
        Increment the size of the specified price level if it exists else set
        price level to the absolute value of the size
        :param pair: str
        :param side: str
        :param price: decimal.Decimal
        :param size: str/float/decimal-> adds this amount to book
        :return: bool-> price exists
        """
        key = self.make_key(pair, side=side)
        return await self.pool.evalsha(
            self.pool.incr_if_exists_else_set_abs,
            keys=[key],
            args=[str(price), str(size)]
        ) is not None

    async def decrement_and_remove_if_zero(self, pair: str, side: str, price: Decimal, size)-> bool:
        """
        subtract size from size at price level and then remove if the decremented size is 0
        :param pair: str
        :param side: str
        :param price: decimal.Decimal
        :param size: negates this amount from book
        :return: bool-> removed
        """
        key = self.make_key(pair, side=side)
        return await self.pool.evalsha(
            self.pool.decr_and_remove_if_zero,
            keys=[key],
            args=[str(price), str(size)]
        ) is not None
