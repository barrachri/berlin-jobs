# coding: utf-8
"""Exploratory data analysis"""


import os
import logging
import glob
import json
import time
from ast import literal_eval
from functools import partial

import numpy as np
import pandas as pd
from tornado import ioloop
from tornado import gen
from tornado import httpclient


log = logging.getLogger(__name__)
logging.basicConfig(format='%(message)s', level=logging.INFO)

now = time.time()
log.info("Cleaning pipeline process.....")

BASE = os.path.dirname(os.path.abspath(__name__))
RESULT_PARQUET = os.path.join(BASE, "data/cleaned/offers.parquet")
folder = os.path.join(BASE, "data/raw")
files = glob.glob(os.path.join(folder, "csv/*/*.csv"))

log.info(f"Base folder: {BASE}")
log.info(f"Number of files available: {len(files)}")


log.info(f"Loading and concatenating files.....")

# Read all the files and create an array of DFs
dataframes = [pd.read_csv(fp, parse_dates=['published_on', 'updated_on']) for fp in files]
# Put the DFs together
df = pd.concat(dataframes, ignore_index=True)

# Drop all the offers with the same id
df = df.drop_duplicates(['job_id'])

# Expanding tags
df['tags'] = pd.Series([literal_eval(x[1]) for x in df['tags'].iteritems()], index=df['tags'].index)
tags = df['tags'].apply(pd.Series)
tags = tags.rename(columns = lambda x : 'tag_' + str(x))

# Add tags features
df = pd.concat([df, tags], axis=1)
df = pd.wide_to_long(df, stubnames='tag_', i='job_id', j='tags_')
df = df.reset_index()
df['tags_'] = df['tags_'].astype(np.uint16)
df = df.rename(columns={"tags_": "tag_order", "tag_": "tag"})

# Remove tags duplicates and tags column
df = df[~((df['tag'].isnull()) & (df['tag_order'] > 0))].drop(labels="tags", axis=1)


log.info("Checking tags....")
# Check if the are unknown tags
unique_tags = df[df["tag"].notnull()]["tag"].unique()
unique_tag_id = list(filter(lambda x: isinstance(x, np.float), unique_tags))
tags_file = os.path.join(folder, "csv/berlinstartupjobs/tags/tags_list.csv")
df_tags = pd.read_csv(tags_file, index_col=["tag_id"])
tags_to_name = df_tags.to_dict(orient='dict')['tag_name']
new_tags = set(map(int, unique_tag_id)) - tags_to_name.keys()


new_tags_to_name = {}

@gen.coroutine
def fetcher(tags):
    """Call the api endpoint to get the name of each tag."""

    now = time.time()

    log.info(f"Fetching {len(tags)} tags.......")

    @gen.coroutine
    def async_client(url):

        try:
            response = yield httpclient.AsyncHTTPClient().fetch(url)
        except httpclient.HTTPError as err:
            # HTTPError is raised for non-200 responses; the response
            # can be found in e.response.
            print("Error: " + str(err))
        return response

    log.info("Fetched tag...", end="")

    for tag in tags:
        _tag = int(tag)
        url = f"http://berlinstartupjobs.com/wp-json/wp/v2/tags/{_tag}"

        response = yield async_client(url)

        json_body = json.loads(response.body)
        new_tags_to_name[_tag] = json_body['slug']

        log.info(f"{_tag}..", end="")

    duration = time.time() - now
    log.info(f"\nTags fetched in {duration} secs")

if new_tags:
    log.info(f"Found {len(new_tags)} new tags, pulling info from the berlinstartup....")
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(partial(fetcher, tags=new_tags))

# Update tags and save the list
tags_to_name.update(new_tags_to_name)
df_tags = pd.DataFrame.from_dict(tags_to_name, orient='index')
df_tags = df_tags.reset_index()
df_tags.columns = ["tag_id", "tag_name"]
df_tags.to_csv(tags_file, index=False)

# Change tags from number to word
def change_value(x):
    value = tags_to_name.get(x)
    if value is not None:
        return value
    return x

df['tag'] = df['tag'].apply(change_value)
df = df.drop(labels="tag_order", axis=1).reset_index(drop=True)

# Save the csv as a unique file in part
df.to_parquet(RESULT_PARQUET, engine="pyarrow")

log.info(f"File saved as: {RESULT_PARQUET}")

total_time = time.time() - now
log.info(f"Total time: {total_time}")
