from decimal import Decimal
import asyncio

from sortedcontainers import SortedDict as sd

from cryptofeed.defines import BID, ASK
from cryptofeed.order_books import SortedDictOrderBook


loop = asyncio.get_event_loop()


def test_set():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('ABC', BID, '0.00135', '4.56'))
    loop.run_until_complete(book.set('ABC', BID, '0.00133', '1.234'))

    loop.run_until_complete(book.set('ABC', ASK, '0.00140', '3.832'))
    loop.run_until_complete(book.set('ABC', ASK, '0.00141', '4.56'))
    loop.run_until_complete(book.set('ABC', ASK, '0.00142', '1.234'))

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


def test_dunder_set_get_item():
    book = SortedDictOrderBook('test')
    new = {
        BID: {
            '0.00134': '3.832',
            '0.00135': '4.56',
        },
        ASK: {
            '0.00140': '3.832',
            '0.00141': '4.56',
        }
    }
    expected = {
        BID: sd({
            Decimal('0.00134'): Decimal('3.832'),
            Decimal('0.00135'): Decimal('4.56'),
        }),
        ASK: sd({
            Decimal('0.00140'): Decimal('3.832'),
            Decimal('0.00141'): Decimal('4.56'),
        })
    }
    book['ABC'] = new
    assert book['ABC'] == expected


def test_dunder_len():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('DEF', BID, '0.00135', '4.56'))
    loop.run_until_complete(book.set('GHI', BID, '0.00133', '1.234'))
    assert len(book) == 3


def test_dunder_del_item():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('DEF', BID, '0.00135', '4.56'))
    loop.run_until_complete(book.set('GHI', BID, '0.00133', '1.234'))
    assert 'GHI' in book
    del book['GHI']
    assert 'GHI' not in book


def test_dunder_contains():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('DEF', BID, '0.00135', '4.56'))
    loop.run_until_complete(book.set('GHI', BID, '0.00133', '1.234'))
    assert 'ABC' in book
    assert 'DEF' in book
    assert 'GHI' in book


def test_get():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('ABC', BID, '0.00135', '4.56'))

    assert loop.run_until_complete(book.get('ABC', BID, '0.00134')) == Decimal('3.832')
    assert loop.run_until_complete(book.get('ABC', BID, '0.00135')) == Decimal('4.56')
    assert loop.run_until_complete(book.get('ABC', BID, '123.45', default='default')) == 'default'


def test_get_pair_side():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('ABC', BID, '0.00135', '4.56'))
    loop.run_until_complete(book.set('ABC', BID, '0.00133', '1.234'))
    pair_side = sd({
        Decimal('0.00134'): Decimal('3.832'),
        Decimal('0.00135'): Decimal('4.56'),
        Decimal('0.00133'): Decimal('1.234'),
    })
    assert loop.run_until_complete(book.get_pair_side('ABC', BID)) == pair_side


def test_price_exists():
    book = SortedDictOrderBook('test')
    assert loop.run_until_complete(book.price_exists('ABC', BID, '0.00134')) is False
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    assert loop.run_until_complete(book.price_exists('ABC', BID, '0.00134')) is True


def test_delete_pair():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('ABC', BID, '0.00135', '4.56'))
    loop.run_until_complete(book.set('ABC', BID, '0.00133', '1.234'))

    loop.run_until_complete(book.set('ABC', ASK, '0.00140', '3.832'))
    loop.run_until_complete(book.set('ABC', ASK, '0.00141', '4.56'))
    loop.run_until_complete(book.set('ABC', ASK, '0.00142', '1.234'))
    assert len(book) == 1
    loop.run_until_complete(book.delete_pair('ABC'))
    assert book['ABC'] == {BID: sd(), ASK: sd()}


def test_increment():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('ABC', BID, '0.00133', '1.234'))
    loop.run_until_complete(book.increment('ABC', BID, '0.00134', '-0.832'))
    loop.run_until_complete(book.increment('ABC', BID, '0.00133', '0.766'))

    assert loop.run_until_complete(book.get('ABC', BID, '0.00133')) == Decimal('2.0')
    assert loop.run_until_complete(book.get('ABC', BID, '0.00134')) == Decimal('3.0')


def test_remove():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '3.832'))
    loop.run_until_complete(book.set('ABC', BID, '0.00133', '1.234'))
    loop.run_until_complete(book.remove('ABC', BID, '0.00133'))

    assert book == {'ABC': {BID: sd({Decimal('0.00134'): Decimal('3.832')}), ASK: sd({})}}


def test_remove_if_zero_size():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '0.0'))
    loop.run_until_complete(book.set('ABC', BID, '0.00133', '1.234'))
    assert loop.run_until_complete(book.get('ABC', BID, '0.00134')) == Decimal('0.0')

    loop.run_until_complete(book.remove_if_zero_size('ABC', BID, '0.00134'))
    assert loop.run_until_complete(book.get('ABC', BID, '0.00134', default='default')) == 'default'

    loop.run_until_complete(book.remove_if_zero_size('ABC', BID, '0.00133'))
    assert loop.run_until_complete(book.get('ABC', BID, '0.00133')) == Decimal('1.234')


def test_incr_if_exists():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '4.81'))
    loop.run_until_complete(book.set('ABC', BID, '0.00133', '1.234'))

    loop.run_until_complete(book.increment_if_exists('ABC', BID, '0.00134', '0.19'))
    assert loop.run_until_complete(book.get('ABC', BID, '0.00134')) == Decimal('5.0')

    loop.run_until_complete(book.increment_if_exists('ABC', BID, '0.00131', '2.0'))
    assert loop.run_until_complete(book.get('ABC', BID, '0.00131')) is None


def test_incr_if_exists_else_set_abs():
    book = SortedDictOrderBook('test')
    loop.run_until_complete(book.set('ABC', BID, '0.00134', '4.81'))
    loop.run_until_complete(book.set('ABC', BID, '0.00133', '1.234'))

    loop.run_until_complete(book.increment_if_exists_else_set_abs('ABC', BID, '0.00134', '-0.81'))
    assert loop.run_until_complete(book.get('ABC', BID, '0.00134')) == Decimal('4.0')

    loop.run_until_complete(book.increment_if_exists_else_set_abs('ABC', BID, '0.00131', '-2.0'))
    assert loop.run_until_complete(book.get('ABC', BID, '0.00131')) == Decimal('2.0')
