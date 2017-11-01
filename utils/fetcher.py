
# coding: utf-8

# This notebook collects data from stackoverflow and berlinstartupjobs.

# In[11]:

import os
from datetime import datetime
from functools import partial
from tornado import gen
from tornado import ioloop
from tornado import httpclient


BASE = os.path.dirname(os.path.abspath(__name__))

# In[12]:


@gen.coroutine
def main(targets):

    for name,values in targets.items():
        print(f"{name}")
        url = values['url']
        folder = values['folder']
        extension = values["extension"]

        now = datetime.utcnow()
        response = yield fetcher(url)

        file_name = "".join((folder, now.strftime("%Y-%m-%d-%H-%M"), ".", extension))
        with open(file_name, mode='wb') as f:
            f.write(response.body)

        print(f"\tFetched and saved as: {file_name}")


# In[13]:


@gen.coroutine
def fetcher(url):
    print(f"\tFetching: {url}")
    response = yield httpclient.AsyncHTTPClient().fetch(url)
    return response


# In[14]:


targets = {}

# In[15]:

# Fetching from stackoverflow
url = "https://stackoverflow.com/jobs/feed?l=Berlino%2c+Germania&d=100&u=Km"
folder = os.path.join(BASE, "data/raw/original/stackoverflow/")
targets["stackoverflow"] = {"url": url, "folder": folder, "extension": "xml"}


# In[16]:

# Fetching from berlinstartupjobs
url = "http://berlinstartupjobs.com/wp-json/wp/v2/posts?categories=9&per_page=100"
folder = os.path.join(BASE, "data/raw/original/berlinstartupjobs/")
targets["berlinstartupjobs"] = {"url": url, "folder": folder, "extension": "json"}


# In[17]:

io_loop = ioloop.IOLoop.current()
io_loop.run_sync(partial(main, targets=targets))
