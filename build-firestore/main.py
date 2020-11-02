import argparse
import re


from tqdm import tqdm
from os import path, makedirs
from typing import Mapping
from nanoid import generate
from google.cloud.storage import Client as Storage
from google.cloud.storage.blob import Blob
from google.cloud.firestore import Client as Firestore
from google.cloud.firestore import ArrayUnion


DEFAULT_BUCKET = 'web-searcher-cloud1'
INVINDEX_OUTPUT = 'inv-index-output'
PAGERANK_OUTPUT = 'page-rank-output'
VALID_PART = re.compile('.*/part-r-[0-9]*$')
VALID_WORD = re.compile('^[a-zA-Z0-9][a-zA-Z0-9-]*$')
DOWNLOADS = 'downloads'


def generate_id():
    return generate('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 14)


def get_website_id(website: str, websites: Mapping[str, str], db: Firestore, create: bool):
    if website in websites:
        return websites[website]
    elif create:
        website_id = generate_id()
        website_doc = db.collection('websites').document(website_id)
        website_doc.set({
            'url': website
        })

        websites[website] = website_id
        return website_id
    else:
        return None


def maybe_download_blob(blob: Blob, storage: Storage, prefix):
    if not path.exists(path.join(DOWNLOADS, blob.name)):
        print(f'{prefix}: downloading file: {blob.name}')
        makedirs(path.join(DOWNLOADS, path.dirname(blob.name)), exist_ok=True)
        blob.download_to_filename(path.join(DOWNLOADS, blob.name))
    else:
        print(f'{prefix}: file already downloaded: {blob.name}')


def open_downloaded_blob(blob: Blob):
    return open(path.join(DOWNLOADS, blob.name), mode='r')


def push_inv_index(websites: Mapping[str, str], db: Firestore, storage: Storage):
    blobs = storage.list_blobs(DEFAULT_BUCKET, prefix=INVINDEX_OUTPUT)
    inv_index_collection = db.collection('inv-index')

    for blob in blobs:
        if not VALID_PART.match(blob.name):
            continue

        maybe_download_blob(blob, storage, prefix='inv-index')
        with open_downloaded_blob(blob) as file:
            print(f'inv-index: processing file: {blob.name}')
            for line in file:
                key_value = line.split('\t')
                word, urls = key_value[0], key_value[1]

                if not VALID_WORD.match(word):
                    continue

                print(f'inv-index: processing word: {word}', end='\x1b[1K\r')
                websites_ids = []

                for url in urls.split('|'):
                    website_id = get_website_id(url, websites, db, create=True)
                    websites_ids.append(website_id)

                word_doc = inv_index_collection.document(word)
                word_doc.set(
                    {'websites': ArrayUnion(websites_ids)}, merge=True)


def push_page_rank(websites: Mapping[str, str], db: Firestore, storage: Storage):
    blobs = storage.list_blobs(DEFAULT_BUCKET, prefix=PAGERANK_OUTPUT)
    page_rank_collection = db.collection('page-rank')

    for blob in blobs:
        if not VALID_PART.match(blob.name):
            continue

        maybe_download_blob(blob, storage, prefix='page-rank')
        with open_downloaded_blob(blob) as file:
            print(f'page-rank: processing file: {blob.name}')
            for line in file:
                key = line.split('\t')[0]
                url_and_pr = key.split('|')
                url, pr = url_and_pr[0], float(url_and_pr[1])

                website_id = get_website_id(url, websites, db, create=False)

                if website is not None:
                    print(f'page-rank: processing url: {url}', end='\x1b[1K\r')
                    pr_doc = page_rank_collection.doc(website_id)
                    pr_doc.set({'value': pr}, merge=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build Firestore database')
    parser.add_argument('--project-id', type=str,
                        required=True, help='Project id')

    args = parser.parse_args()
    print(args.project_id)

    db = Firestore(project=args.project_id)
    storage = Storage(project=args.project_id)
    websites = {}

    push_inv_index(websites, db, storage)
    push_page_rank(websites, db, storage)
