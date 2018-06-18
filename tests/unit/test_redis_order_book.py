from decimal import Decimal
import asyncio

from sortedcontainers import SortedDict as sd

from cryptofeed.defines import BID, ASK
from cryptofeed.order_books import RedisOrderBook


loop = asyncio.get_event_loop()

FLOAT_ERROR_MARGIN = Decimal('0.00000000000001')


def is_within_error_margin(got, expected):
    return (expected - FLOAT_ERROR_MARGIN) <= got <= (expected + FLOAT_ERROR_MARGIN)


def test_set():
    book = RedisOrderBook('test')
    loop.run_until_complete(asyncio.gather(book.set('ABC', BID, '0.00134', '3.832'),
                            book.set('ABC', BID, '0.00135', '4.56'),
                            book.set('ABC', BID, '0.00133', '1.234'),
                            book.set('ABC', ASK, '0.00142', '1.234'),
                            book.set('ABC', ASK, '0.00140', '3.832'),
                            book.set('ABC', ASK, '0.00141', '4.56')))

    bid_list, ask_list, full_book = loop.run_until_complete(
        asyncio.gather(book.sorted_bids_for_pair('ABC'),
                       book.sorted_asks_for_pair('ABC'),
                       book.get_exchange_book()))
    loop.run_until_complete(book.delete_pair('ABC'))

    assert bid_list == [Decimal('0.00135'), Decimal('0.00134'), Decimal('0.00133')]
    assert ask_list == [Decimal('0.00140'), Decimal('0.00141'), Decimal('0.00142')]
    assert full_book == {
        'ABC': {
            BID: sd({Decimal('0.00134'): Decimal('3.832'),
                     Decimal('0.00135'): Decimal('4.56'),
                     Decimal('0.00133'): Decimal('1.234')}),
            ASK: sd({Decimal('0.00140'): Decimal('3.832'),
                     Decimal('0.00141'): Decimal('4.56'),
                     Decimal('0.00142'): Decimal('1.234')})
        }
    }


def test_set_get_pair_book():
    book = RedisOrderBook('test')
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

    loop.run_until_complete(book.set_pair_book('ABC', new))
    pair_book = loop.run_until_complete(book.get_pair_book('ABC'))
    loop.run_until_complete(book.delete_pair('ABC'))
    assert pair_book == expected


def test_get():
    book = RedisOrderBook('test')
    loop.run_until_complete(asyncio.gather(
        book.set('ABC', BID, '0.00134', '3.832'),
        book.set('ABC', BID, '0.00135', '4.56')))
    get1, get2, default = loop.run_until_complete(
        asyncio.gather(book.get('ABC', BID, '0.00134'),
                       book.get('ABC', BID, '0.00135'),
                       book.get('ABC', BID, '123.45', default='default')))
    loop.run_until_complete(book.delete_pair('ABC'))

    assert get1 == Decimal('3.832')
    assert get2 == Decimal('4.56')
    assert default == 'default'


def test_get_pair_side():
    book = RedisOrderBook('test')
    expected_pair_side = sd({
        Decimal('0.00134'): Decimal('3.832'),
        Decimal('0.00135'): Decimal('4.56'),
        Decimal('0.00133'): Decimal('1.234'),
    })

    loop.run_until_complete(
        asyncio.gather(book.set('ABC', BID, '0.00134', '3.832'),
                       book.set('ABC', BID, '0.00135', '4.56'),
                       book.set('ABC', BID, '0.00133', '1.234')))
    pair_side_result = loop.run_until_complete(book.get_pair_side('ABC', BID))
    loop.run_until_complete(book.delete_pair('ABC'))
    assert pair_side_result == expected_pair_side


def test_price_exists():
    book = RedisOrderBook('test')
    loop.run_until_complete(asyncio.gather(book.set('ABC', BID, '0.00134', '3.832')))
    doesnt_exist, exists = loop.run_until_complete(
        asyncio.gather(book.price_exists('ABC', BID, '8'),
                       book.price_exists('ABC', BID, '0.00134')))
    loop.run_until_complete(book.delete_pair('ABC'))

    assert doesnt_exist is False
    assert exists is True


def test_delete_pair():
    book = RedisOrderBook('test')
    loop.run_until_complete(
        asyncio.gather(book.set('ABC', BID, '0.00134', '3.832'),
                       book.set('ABC', BID, '0.00135', '4.56'),
                       book.set('ABC', BID, '0.00133', '1.234'),
                       book.set('ABC', ASK, '0.00140', '3.832'),
                       book.set('ABC', ASK, '0.00141', '4.56'),
                       book.set('ABC', ASK, '0.00142', '1.234'))
    )

    pairs = loop.run_until_complete(book.get_pairs())
    loop.run_until_complete(book.delete_pair('ABC'))

    results = asyncio.gather(book.sorted_bids_for_pair('ABC'),
                             book.sorted_asks_for_pair('ABC'),
                             book.get_pair_book('ABC'))
    bid_list, ask_list, pair_book = loop.run_until_complete(results)
    loop.run_until_complete(book.delete_pair('ABC'))

    assert len(pairs) == 1
    assert bid_list == []
    assert ask_list == []
    assert pair_book == {BID: sd(), ASK: sd()}


def test_increment():
    book = RedisOrderBook('test')
    loop.run_until_complete(
        asyncio.gather(book.set('ABC', BID, '0.00134', '3.832'),
                       book.set('ABC', BID, '0.00133', '1.234')))
    loop.run_until_complete(
        asyncio.gather(book.increment('ABC', BID, '0.00134', '-0.832'),
                       book.increment('ABC', BID, '0.00133', '0.766')))
    incr, decr = loop.run_until_complete(
        asyncio.gather(book.get('ABC', BID, '0.00133'),
                       book.get('ABC', BID, '0.00134')))
    loop.run_until_complete(book.delete_pair('ABC'))

    assert is_within_error_margin(incr, Decimal('2.0'))
    assert is_within_error_margin(decr, Decimal('3.0'))


def test_remove():
    book = RedisOrderBook('test')
    loop.run_until_complete(
        asyncio.gather(book.set('ABC', BID, '0.00134', '3.832'),
                       book.set('ABC', BID, '0.00133', '1.234'),
                       book.set('ABC', BID, '0.00132', '2.84')))
    initial_bid_list = loop.run_until_complete(book.sorted_bids_for_pair('ABC'))
    loop.run_until_complete(book.remove('ABC', BID, '0.00133'))
    bid_list_one_removed, full_book = loop.run_until_complete(
        asyncio.gather(book.sorted_bids_for_pair('ABC'),
                       book.get_exchange_book()))
    loop.run_until_complete(book.delete_pair('ABC'))

    assert initial_bid_list == [Decimal('0.00134'), Decimal('0.00133'), Decimal('0.00132')]
    assert bid_list_one_removed == [Decimal('0.00134'), Decimal('0.00132')]
    assert full_book == {
        'ABC':
            {
                BID: sd({
                    Decimal('0.00134'): Decimal('3.832'),
                    Decimal('0.00132'): Decimal('2.84'),
                }),
                ASK: sd({})
            }
    }


def test_remove_if_zero_size():
    book = RedisOrderBook('test')
    setup = asyncio.gather(book.set('ABC', BID, '0.00134', '0.0000000000000000001'),
                           book.set('ABC', BID, '0.00133', '1.234'))
    loop.run_until_complete(setup)
    should_be_zero = loop.run_until_complete(book.get('ABC', BID, '0.00134'))
    loop.run_until_complete(book.remove_if_zero_size('ABC', BID, '0.00134'))
    should_be_default = loop.run_until_complete(book.get('ABC', BID, '0.00134', default='default'))
    loop.run_until_complete(book.remove_if_zero_size('ABC', BID, '0.00133'))
    should_exist = loop.run_until_complete(book.get('ABC', BID, '0.00133'))
    loop.run_until_complete(book.delete_pair('ABC'))

    assert is_within_error_margin(should_be_zero, Decimal('0.0'))
    assert should_be_default == 'default'
    assert should_exist == Decimal('1.234')


def test_incr_if_exists():
    book = RedisOrderBook('test')
    loop.run_until_complete(
        asyncio.gather(book.set('ABC', BID, '0.00134', '4.81'),
                       book.set('ABC', BID, '0.00133', '1.234'),)
    )
    loop.run_until_complete(book.increment_if_exists('ABC', BID, '0.00134', '0.19'))
    incremented = loop.run_until_complete(book.get('ABC', BID, '0.00134'))
    loop.run_until_complete(book.increment_if_exists('ABC', BID, '0.00131', '2.0'))
    not_exists = loop.run_until_complete(book.get('ABC', BID, '0.00131'))
    loop.run_until_complete(book.delete_pair('ABC'))

    assert is_within_error_margin(incremented, Decimal('5.0'))
    assert not_exists is None


def test_incr_if_exists_else_set_abs():
    book = RedisOrderBook('test')
    loop.run_until_complete(
        asyncio.gather(book.set('ABC', BID, '0.00134', '4.81'),
                       book.set('ABC', BID, '0.00133', '1.234')))
    loop.run_until_complete(book.increment_if_exists_else_set_abs('ABC', BID, '0.00134', '-0.81'))
    incremented = loop.run_until_complete(book.get('ABC', BID, '0.00134'))
    loop.run_until_complete(book.increment_if_exists_else_set_abs('ABC', BID, '0.00131', '-2.0'))
    set_abs = loop.run_until_complete(book.get('ABC', BID, '0.00131'))
    loop.run_until_complete(book.delete_pair('ABC'))

    assert is_within_error_margin(incremented, Decimal('4.0'))
    assert is_within_error_margin(set_abs, Decimal('2.0'))
