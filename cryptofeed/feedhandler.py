'''
Copyright (C) 2017-2018  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
import logging
from datetime import datetime as dt
from datetime import timedelta

import websockets
from websockets import ConnectionClosed

from cryptofeed.defines import TICKER
from cryptofeed import Gemini
from .nbbo import NBBO


FORMAT = '%(asctime)-15s : %(levelname)s : %(message)s'
logging.basicConfig(level=logging.WARNING,
                    format=FORMAT,
                    handlers=[logging.FileHandler('feedhandler.log'),
                              logging.StreamHandler()])

LOG = logging.getLogger('feedhandler')


class FeedHandler(object):
    def __init__(self, retries=10):
        self.feeds = []
        self.qos = {}
        self.retries = retries

    def add_feed(self, feed):
        self.feeds.append(feed)
        self.qos[feed.id] = None

    def add_nbbo(self, feeds, pairs, callback):
        cb = NBBO(callback, pairs)
        for feed in feeds:
            self.add_feed(feed(channels=[TICKER], pairs=pairs, callbacks={TICKER: cb}))

    def run(self):
        if self.feeds == []:
            LOG.error('No feeds specified')
            raise ValueError("No feeds specified")

        try:
            asyncio.get_event_loop().run_until_complete(self._run())
        except KeyboardInterrupt:
            LOG.info("Keyboard Interrupt received - shutting down")
            pass
        except Exception as e:
            LOG.error("Unhandled exception: %s", str(e))

    def _run(self):
        feeds = [asyncio.ensure_future(self._connect(feed)) for feed in self.feeds]
        _, _ = yield from asyncio.wait(feeds)
    
    async def _watch(self, feed_id, websocket):
        print("Started a watcher")
        while websocket.open:
            print("Checking")
            if self.qos[feed_id]:
                if dt.utcnow() - timedelta(seconds=10) > self.qos[feed_id]:
                    print("Error")
                    await websocket.close()
                    print("Closed")
            await asyncio.sleep(5)
        print("Watcher exiting")

    async def _connect(self, feed):
        retries = 0
        delay = 1.0
        while retries <= self.retries:
            self.qos[feed.id] = None
            try:
                print("Connecting...")
                async with websockets.connect(feed.address) as websocket:
                    await feed.subscribe(websocket)
                    asyncio.ensure_future(self._watch(feed.id, websocket))
                    await self._handler(websocket, feed.message_handler, feed.id)
            except ConnectionClosed as e:
                LOG.error("Feed {} encountered connection issue {} - reconnecting...".format(feed.id, repr(e)))
                await asyncio.sleep(delay)
                retries += 1
                delay = delay * 2
            except Exception as e:
                LOG.error("Feed {} encountered an exception {} - reconnecting...".format(feed.id, repr(e)))
                await asyncio.sleep(delay)
                retries += 1
                delay = delay * 2
        LOG.error("Feed {} failed to reconnect after {} retries - exiting".format(feed.id, retries))

    async def _handler(self, websocket, handler, feed_id):
        async for message in websocket:
            await handler(message)
            self.qos[feed_id] = dt.utcnow()
