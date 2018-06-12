from decimal import Decimal

from sortedcontainers import SortedDict as sd

from cryptofeed.defines import BID, ASK
from cryptofeed.order_books.base_order_book import OrderBookBase


class SortedDictOrderBook(OrderBookBase):
    def __init__(self, exchange, *args, **kwargs):
        super(SortedDictOrderBook, self).__init__(exchange)
        self._book = {}

    def __setitem__(self, key, value):
        # assert isinstance(value, dict) and \
        #        set(value.keys()) == {BID, ASK} and \
        #        isinstance(value[BID], sd) and \
        #        isinstance(value[ASK], sd), \
        #        'SortedDictOrderBook values must be of format: {{ \'bid\': SortedDict(), \'ask\': SortedDict() }} ' \
        #        'Got {0!r} instead.'.format(value)
        self._book.__setitem__(key, value)

    def __getitem__(self, key):
        if key not in self._book:
            self._book[key] = {BID: sd(), ASK: sd()}
        return self._book.__getitem__(key)

    # @staticmethod
    # def _check_side(side):
    #     if side not in (BID, ASK):
    #         raise ValueError('Side must be either "bid" or "ask". Got {!r}'.format(side))

    def get(self, pair: str, side: str, price, default=None):
        """
        Fetch level from specified book, returns `default` if not found.
        :param pair: str
        :param side: str ('bid', 'ask')
        :param price: decimal/str/float
        :param default: decimal/str/float
        :return: Decimal/None -> size value of level or None
        """
        # self._check_side(side)
        return self[pair][side].get(Decimal(price), default)

    def set_level(self, pair: str, side: str, price, size):
        """
        Sets the price level to the specified size
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal
        :return: None
        """
        # self._check_side(side)
        self[pair][side][Decimal(price)] = Decimal(size)

    def increment_level(self, pair: str, side: str, price, size):
        """
        Increment the size of the specified price level
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal
        :return: None
        """
        # self._check_side(side)
        self[pair][side][Decimal(price)] += Decimal(size)

    def remove_level(self, pair: str, side: str, price):
        """
        Remove the specified price level from the book
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :return: None
        """
        # self._check_side(side)
        del self[pair][side][Decimal(price)]

    def clear_pair(self, pair: str):
        """
        Clear the book for the specified pair
        :param pair: str
        :return: None
        """
        self[pair] = {BID: sd(), ASK: sd()}
