class OrderBookBase(object):
    def __init__(self, exchange):
        self.exchange = exchange
        self._book = None

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

    def __getitem__(self, item):
        raise NotImplemented

    def __setitem__(self, key, value):
        raise NotImplemented

    def get(self, pair: str, side: str, price, default=None):
        """
        Fetch level from specified book, returns `default` if not found.
        :param pair: str
        :param side: str ('bid', 'ask')
        :param price: decimal/str/float
        :param default: decimal/str/float
        :return: Decimal/None -> size value of level or None
        """
        raise NotImplemented

    def set_level(self, pair: str, side: str, price, size):
        """
        Sets the price level to the specified size
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal
        :return: None
        """
        raise NotImplemented

    def increment_level(self, pair: str, side: str, price, size):
        """
        Increment the size of the specified price level
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal
        :return: None
        """
        raise NotImplemented

    def remove_level(self, pair: str, side: str, price):
        """
        Remove the specified price level from the book
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :return: None
        """
        raise NotImplemented

    def clear_pair(self, pair: str):
        """
        Clear the book for the specified pair
        :param pair: str
        :return: None
        """
        raise NotImplemented
