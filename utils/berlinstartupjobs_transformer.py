"""Transformer for Berlinstartupjobs."""

import os
import json
import logging
import glob
import csv
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)
logging.basicConfig(format='%(message)s', level=logging.INFO)

log.info(f"Transforming berlinstartupjobs offers....")

BASE = os.path.dirname(os.path.abspath(__name__))
folder = os.path.join(BASE, "data/raw/original/berlinstartupjobs")
csv_folder = os.path.join(BASE, "data/raw/csv/berlinstartupjobs")

files = glob.glob(os.path.join(folder, "*.json"))

log.info(f"Folder: {folder}")
log.info(f"Total number of files: {len(files)}")


# some easy cleanings
def clean_entry(entries):

    _entries = []

    for entry in entries:

        _entry = {}

        _entry['job_id'] = entry['id']

        _entry['published_on'] = entry['date_gmt']

        _entry['updated_on'] = entry['modified_gmt']

        title = entry['title']['rendered'].split(" // ")
        _entry['role'], _entry['company'] = title

        _entry['slug'] = entry['slug']

        _entry['link'] = entry['link']

        _entry['description'] = BeautifulSoup(
            entry['content']['rendered'], "html5lib").text

        _entry['tags'] = entry['tags']

        _entries.append(_entry)

    return _entries


log.info("Converting json to csv....")

for index, file in enumerate(files, 1):

    filename = os.path.basename(file)
    csv_filename = "".join((os.path.splitext(filename)[0], ".csv"))
    csv_path = os.path.join(csv_folder, csv_filename)

    if os.path.exists(csv_path):
        log.info(f"\t{index}) {csv_filename} already exist")
    else:

        with open(file, mode="r", encoding="utf-8") as f:
            try:
                entries = json.loads(f.read(), encoding="utf-8")
            except json.JSONDecodeError:
                print(f"Error: {file}")
                continue

        cleaned_entries = clean_entry(entries)
        del entries

        with open(csv_path, 'w', encoding="utf-8") as f:
            w = csv.DictWriter(f, cleaned_entries[0].keys())
            w.writeheader()
            w.writerows(cleaned_entries)
        log.info(f"\t{index}) {csv_filename} saved")
