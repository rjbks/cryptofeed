from decimal import Decimal

from sortedcontainers import SortedDict as sd

from cryptofeed.defines import BID, ASK
from cryptofeed.order_books.base_order_book import OrderBookBase


class SortedDictOrderBook(OrderBookBase):
    def __init__(self, exchange, *args, **kwargs):
        super(SortedDictOrderBook, self).__init__(exchange)
        self._book = {}

    def __str__(self):
        return self._book.__str__()

    def __repr__(self):
        return self._book.__repr__()

    def __delitem__(self, key):
        del self._book[key]

    def __len__(self):
        return len(self._book)

    def __cmp__(self, other):
        return self._book.__cmp__(other)

    def __eq__(self, other):
        return self._book.__eq__(other)

    def __contains__(self, item):
        return item in self._book

    def __iter__(self):
        return iter(self._book)

    def __setitem__(self, key, value):
        bids = value.get(BID, {})
        asks = value.get(ASK, {})
        assert isinstance(bids, (dict, sd)) and isinstance(asks, (dict, sd)), \
            'BID and ASK keys ("bid"/"ask") must have `dict` value. Got types {}(BID)/{}(ASK)'.format(
                bids.__class__.__name__,
                asks.__class__.__name__
            )
        book = {
            BID: sd({
                Decimal(price): Decimal(size)
                for price, size in bids.items()
            }),
            ASK: sd({
                Decimal(price): Decimal(size)
                for price, size in asks.items()
            })
        }
        self._book.__setitem__(key, book)

    def __getitem__(self, pair):
        if pair not in self._book:
            self._book[pair] = {BID: sd(), ASK: sd()}
        return self._book.__getitem__(pair)

    async def get_pairs(self)-> list:
        """
        get all pairs in exchange book
        :return: list-> list of all pairs in book
        """
        return list(self._book.keys())

    async def get_pair_book(self, pair: str)-> dict:
        """
        get BID/ASK books for specified pair
        :param pair: str
        :return: tuple-> (pair, dict of BID/ASK SortedDicts
            {BID: sd({p1:s1,...}), ASK: sd({...})})
        """
        return self[pair]

    async def get_pair_side(self, pair: str, side: str)-> sd:
        """
        get BID or ASK side of book for specified pair
        :param pair: str
        :param side: str
        :return: SortedDict-> price/size pairs for specified side of pair
        """
        return self[pair][side]

    async def price_exists(self, pair: str, side: str, price: Decimal)-> bool:
        """
        Checks if price exists in specified book
        :param pair: str
        :param side: str
        :param price: str
        :return: bool
        """
        return Decimal(price) in self[pair][side]

    async def delete_pair(self, pair: str)-> None:
        """
        Removes pair from book
        :param pair: str
        :return: None
        """
        self[pair] = {BID: sd(), ASK: sd()}

    async def get(self, pair: str, side: str, price: Decimal, default=None):
        """
        Fetch level from specified book, returns `default` if not found.
        :param pair: str
        :param side: str ('bid', 'ask')
        :param price: decimal/str/float
        :param default: decimal/str/float
        :return: Decimal/None -> size value of level or None
        """
        return self[pair][side].get(Decimal(price), default)

    async def set(self, pair: str, side: str, price: Decimal, size):
        """
        Sets the price level to the specified size
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal
        :return: None
        """
        self[pair][side][Decimal(price)] = Decimal(size)

    async def set_pair_book(self, pair: str, book: dict)-> None:
        """
        set BID/ASK books for specified pair
        deletes all books and corresponding sorted sets for
        the pair in this exchange prior to setting new values
        :param pair: str
        :param book: dict
        :return: None
        """
        self[pair] = book

    async def increment(self, pair: str, side: str, price: Decimal, size):
        """
        Increment the size of the specified price level
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal
        :return: None
        """
        self[pair][side][Decimal(price)] += Decimal(size)

    async def increment_if_exists(self, pair: str, side: str, price: Decimal, size)-> bool:
        """
        Increment the size of the specified price level if it exists
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal-> amount to increment by
        :return: bool-> exists
        """
        price = Decimal(price)
        exists = False
        if price in self[pair][side]:
            exists = True
            self[pair][side][price] += Decimal(size)
        return exists

    async def increment_if_exists_else_set_abs(self, pair: str, side: str, price: Decimal, size)-> bool:
        """
        Increment the size of the specified price level if it exists else set
        price level to the absolute value of the size
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal-> amount to increment by
        :return: bool-> exists
        """
        price = Decimal(price)
        if price in self[pair][side]:
            exists = True
            self[pair][side][price] += Decimal(size)
        else:
            exists = False
            self[pair][side][price] = abs(Decimal(size))
        return exists

    async def decrement_and_remove_if_zero(self, pair: str, side: str, price: Decimal, size)-> bool:
        """
        decrement price level size and then remove if size is 0
        :param pair: str
        :param side: str
        :param price:
        :param size: negates this amount from book
        :return: bool-> removed
        """
        price = Decimal(price)
        size = Decimal(size)
        removed = False
        self[pair][side][price] -= size
        if self[pair][side][price] == 0:
            del self[pair][side][price]
            removed = True
        return removed

    async def remove(self, pair: str, side: str, price: Decimal):
        """
        Remove the specified price level from the book
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :return: None
        """
        del self[pair][side][Decimal(price)]

    async def remove_if_exists(self, pair: str, side: str, price: Decimal)-> bool:
        """
        remove price level if it exists
        redis doesn't throw an error when deleting non-existant fields
        :param pair: str
        :param side: str
        :param price:
        :return: bool-> exists
        """
        price = Decimal(price)
        exists = False
        if self[pair][side].get(price):
            exists = True
            del self[pair][side][price]
        return exists

    async def remove_if_zero_size(self, pair: str, side: str, price: Decimal)-> bool:
        """
        remove price from book if size is 0
        :param pair: str
        :param side: str
        :param price:
        :return: bool-> removed
        """
        price = Decimal(price)
        removed = False
        if self[pair][side].get(price, None) == 0:
            del self[pair][side][price]
            removed = True
        return removed
