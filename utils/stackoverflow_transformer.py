"""Transformer for xml files from stackoverflow."""

import os
import logging
import glob
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import csv
from bs4 import BeautifulSoup


log = logging.getLogger(__name__)
logging.basicConfig(format='%(message)s', level=logging.INFO)

BASE = os.path.dirname(os.path.abspath(__name__))


log.info(f"Transforming stackoverflow offers....")

folder = os.path.join(BASE, "data/raw/original/stackoverflow")
csv_folder = os.path.join(BASE, "data/raw/csv/stackoverflow")

files = glob.glob(os.path.join(folder, "*.xml"))

log.info(f"Folder: {folder}")
log.info(f"Total number of files: {len(files)}")

log.info("Converting xml to csv....")

for index, file in enumerate(files, 1):

    tree = ET.parse(file)
    root = tree.getroot()

    filename = os.path.basename(file)
    csv_filename = "".join((os.path.splitext(filename)[0], ".csv"))
    csv_path = os.path.join(csv_folder, csv_filename)

    if os.path.exists(csv_path):
        log.info(f"\t{index}) {csv_filename} already exist")
    else:
        with open(csv_path, 'w', encoding="utf-8") as csvfile:

            fieldnames = [
                'job_id', 'published_on', 'updated_on', 'company',
                'slug', 'role', 'link', 'description', 'tags'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for elem in root.iter(tag='item'):

                data = {}

                data['job_id'] = elem.find('guid').text

                data['published_on'] = elem.find('pubDate').text

                data['updated_on'] = elem.find(
                    '{http://www.w3.org/2005/Atom}updated').text

                data['company'] = elem.find(
                    '{http://www.w3.org/2005/Atom}author'
                    ).find('{http://www.w3.org/2005/Atom}name').text

                data['slug'] = elem.find('link').text.split("/")[-1]

                data['role'] = elem.find('title').text

                data['link'] = elem.find('link').text

                data['description'] = BeautifulSoup(
                    elem.find('description').text, "html5lib").text

                data['tags'] = [cat.text for cat in elem.findall('category')]

                writer.writerow(data)

            log.info(f"\t{index}) {csv_filename} saved")
