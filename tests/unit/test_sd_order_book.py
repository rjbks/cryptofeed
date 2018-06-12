from decimal import Decimal

from sortedcontainers import SortedDict as sd

from cryptofeed.defines import BID, ASK
from cryptofeed.order_books.sd_order_book import SortedDictOrderBook


def test_set():
    book = SortedDictOrderBook('test')
    book.set_level('ABC', BID, '0.00134', '3.832')
    book.set_level('ABC', BID, '0.00135', '4.56')
    book.set_level('ABC', BID, '0.00133', '1.234')

    book.set_level('ABC', ASK, '0.00140', '3.832')
    book.set_level('ABC', ASK, '0.00141', '4.56')
    book.set_level('ABC', ASK, '0.00142', '1.234')

    assert book == {
        'ABC': {
            BID: sd({Decimal('0.00134'): Decimal('3.832'),
                     Decimal('0.00135'): Decimal('4.56'),
                     Decimal('0.00133'): Decimal('1.234')}),
            ASK: sd({Decimal('0.00140'): Decimal('3.832'),
                     Decimal('0.00141'): Decimal('4.56'),
                     Decimal('0.00142'): Decimal('1.234')})
        }
    }


# def test_set_side_error():
#     key = 'error'
#     try:
#         book = SortedDictOrderBook('test')
#         book.set_level('ABC', key, '0.00134', '3.832')
#     except ValueError as e:
#         pass
#     else:
#         raise AssertionError(
#             'Key must be either "bid" or "ask". Got {!r} instead, and ValueError not raised'.format(key)
#         )


def test_get():
    book = SortedDictOrderBook('test')
    book.set_level('ABC', BID, '0.00134', '3.832')
    book.set_level('ABC', BID, '0.00135', '4.56')

    assert book.get('ABC', BID, '0.00134') == Decimal('3.832')
    assert book.get('ABC', BID, '0.00135') == Decimal('4.56')
    assert book.get('ABC', BID, '123.45', default='default') == 'default'


def test_increment():
    book = SortedDictOrderBook('test')
    book.set_level('ABC', BID, '0.00134', '3.832')
    book.set_level('ABC', BID, '0.00133', '1.234')
    book.increment_level('ABC', BID, '0.00134', '-0.832')
    book.increment_level('ABC', BID, '0.00133', '0.766')

    assert book.get('ABC', BID, '0.00133') == Decimal('2.0')
    assert book.get('ABC', BID, '0.00134') == Decimal('3.0')


def test_remove():
    book = SortedDictOrderBook('test')
    book.set_level('ABC', BID, '0.00134', '3.832')
    book.set_level('ABC', BID, '0.00133', '1.234')
    book.remove_level('ABC', BID, '0.00133')

    assert book == {'ABC': {BID: sd({Decimal('0.00134'): Decimal('3.832')}), ASK: sd({})}}


def test_clear_pair():
    book = SortedDictOrderBook('test')
    book.set_level('ABC', BID, '0.00134', '3.832')
    book.set_level('ABC', BID, '0.00133', '1.234')

    assert book == {
        'ABC': {
            BID: sd({
                Decimal('0.00134'): Decimal('3.832'),
                Decimal('0.00133'): Decimal('1.234')}),
            ASK: sd({})
        }
    }

    book.clear_pair('ABC')

    assert book['ABC'] == {BID: sd(), ASK: sd()}
