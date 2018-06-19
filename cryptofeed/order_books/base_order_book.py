class OrderBookBase(object):
    def __init__(self, exchange):
        self.exchange = exchange
        self._book = None

    async def get_pair_book(self, pair: str)-> dict:
        """
        get BID/ASK books for specified pair
        :param pair: str
        :return: tuple-> (pair, dict of BID/ASK SortedDicts
            {BID: sd({p1:s1,...}), ASK: sd({...})})
        """
        raise NotImplemented

    async def set_pair_book(self, pair: str, book: dict)-> None:
        """
        set BID/ASK books for specified pair
        deletes all books and corresponding sorted sets for
        the pair in this exchange prior to setting new values
        :param pair: str
        :param book: dict
        :return: None
        """
        raise NotImplemented

    async def get_pair_side(self, pair: str, side: str)-> dict:
        """
        get BID or ASK side of book for specified pair
        :param pair: str
        :param side: str
        :return: SortedDict-> price/size pairs for specified side of pair
        """
        raise NotImplemented

    async def price_exists(self, pair: str, side: str, price: str)-> bool:
        """
        Checks if price exists in specified book
        :param pair: str
        :param side: str
        :param price: str
        :return: bool
        """
        raise NotImplemented

    async def delete_pair(self, pair: str)-> None:
        """
        Removes pair from book
        :param pair: str
        :return: None
        """
        raise NotImplemented

    async def get(self, pair: str, side: str, price, default=None):
        """
        Fetch level from specified book, returns `default` if not found.
        :param pair: str
        :param side: str ('bid', 'ask')
        :param price: decimal/str/float
        :param default: decimal/str/float
        :return: Decimal/None -> size value of level or None
        """
        raise NotImplemented

    async def set(self, pair: str, side: str, price, size):
        """
        Sets the price level to the specified size
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal
        :return: None
        """
        raise NotImplemented

    async def increment(self, pair: str, side: str, price, size):
        """
        Increment the size of the specified price level
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal-> adds this amount to book
        :return: None
        """
        raise NotImplemented

    async def increment_if_exists(self, pair: str, side: str, price, size)-> bool:
        """
        Increment the size of the specified price level if it exists
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal-> adds this amount to book
        :return: bool-> price exists
        """
        raise NotImplemented

    async def increment_if_exists_else_set_abs(self, pair: str, side: str, price, size)-> bool:
        """
        Increment the size of the specified price level if it exists else set
        price level to the absolute value of the size
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal-> adds this amount to book
        :return: bool-> exists
        """
        raise NotImplemented

    async def decrement_and_remove_if_zero(self, pair: str, side: str, price, size)-> bool:
        """
        decrement price level size and then remove if size is 0
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :param size: str/float/decimal-> negates this amount from book
        :return: bool-> removed
        """
        raise NotImplemented

    async def remove(self, pair: str, side: str, price):
        """
        Remove the specified price level from the book
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :return: None
        """
        raise NotImplemented

    async def remove_if_exists(self, pair: str, side: str, price)-> bool:
        """
        remove price level if it exists
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :return: bool-> removed
        """
        raise NotImplemented

    async def remove_if_zero_size(self, pair: str, side: str, price)-> bool:
        """
        remove price from book if size is 0
        :param pair: str
        :param side: str
        :param price: str/float/decimal
        :return: bool-> removed
        """
        raise NotImplemented
