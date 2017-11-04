"""Collects data from stackoverflow and berlinstartupjobs."""

import os
from datetime import datetime
from functools import partial
import logging
from tornado import gen
from tornado import ioloop
from tornado import httpclient

log = logging.getLogger(__name__)
logging.basicConfig(format='%(message)s', level=logging.INFO)

BASE = os.path.dirname(os.path.abspath(__name__))


@gen.coroutine
def main(targets):
    """Main importer."""
    for name, values in targets.items():
        log.info(f"{name}")
        url = values['url']
        folder = values['folder']
        extension = values["extension"]

        now = datetime.utcnow()
        response = yield fetcher(url)

        file_name = "".join((
            folder, now.strftime("%Y-%m-%d-%H-%M"), ".", extension
            ))
        with open(file_name, mode='wb') as f:
            f.write(response.body)

        log.info(f"\tFetched and saved as: {file_name}")


@gen.coroutine
def fetcher(url):
    """Fetcher for websites."""
    log.info(f"\tFetching: {url}")
    response = yield httpclient.AsyncHTTPClient().fetch(url)
    return response


targets = {}

url = "https://stackoverflow.com/jobs/feed?l=Berlino%2c+Germania&d=100&u=Km"
folder = os.path.join(BASE, "data/raw/original/stackoverflow/")
targets["stackoverflow"] = {"url": url, "folder": folder, "extension": "xml"}

url = "http://berlinstartupjobs.com/wp-json/wp/v2/posts?categories=9&per_page=100"  # noqa
folder = os.path.join(BASE, "data/raw/original/berlinstartupjobs/")
targets["berlinstartupjobs"] = {"url": url, "folder": folder, "extension": "json"}  # noqa

io_loop = ioloop.IOLoop.current()
io_loop.run_sync(partial(main, targets=targets))
