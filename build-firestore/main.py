import argparse
import re

from os import path, makedirs
from typing import Mapping
from nanoid import generate
from google.cloud.storage import Client as Storage
from google.cloud.storage.blob import Blob
from google.cloud.firestore import Client as Firestore
from google.cloud.firestore import ArrayUnion
from google.cloud.firestore import DocumentReference
from halo import Halo

DEFAULT_BUCKET = 'web-searcher-cloud1'
INVINDEX_OUTPUT = 'inv-index-output'
PAGERANK_OUTPUT = 'page-rank-output'
VALID_PART = re.compile('.*/part-r-[0-9]*$')
VALID_WORD = re.compile('^[a-zA-Z0-9][a-zA-Z0-9-]*$')
DOWNLOADS = 'downloads'
BATCH_MAX_SIZE = 500


def generate_id():
    return generate('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 14)


class AutoBatch:
    def __init__(self, db: Firestore):
        self.db = db
        self.batch = db.batch()
        self.bsize = 0

    def create(self, ref: DocumentReference, data: any):
        self.batch.create(ref, data)
        self.bsize += 1

        if self.bsize == BATCH_MAX_SIZE:
            self.batch.commit()
            self.batch = self.db.batch()
            self.bsize = 0

    def finish(self):
        if self.bsize != 0:
            self.batch.commit()


class Websites:
    def __init__(self, db: Firestore):
        self.batch = AutoBatch(db)
        self.webids: Mapping[str, str] = {}

    def __getitem__(self, key: str):
        return self.webids.__getitem__(key)

    def __contains__(self, key: str):
        return self.webids.__contains__(key)

    def finish(self):
        self.batch.finish()

    def store(self, website: str):
        webid = generate_id()
        website_doc = db.collection('websites').document(webid)
        self.batch.create(website_doc, {'url': website})
        self.webids[website] = webid

        return webid


def get_website_id(website: str, websites: Websites, create: bool):
    if website in websites:
        return websites[website]
    elif create:
        return websites.store(website)
    else:
        return None


def maybe_download_blob(blob: Blob, storage: Storage, prefix):
    spinner = Halo(text=f'{prefix}: downloading file: {blob.name}')
    spinner.start()

    if not path.exists(path.join(DOWNLOADS, blob.name)):
        makedirs(path.join(DOWNLOADS, path.dirname(blob.name)), exist_ok=True)
        blob.download_to_filename(path.join(DOWNLOADS, blob.name))
        spinner.succeed(f'{prefix}: successfully downloaded: {blob.name}')
    else:
        spinner.succeed(f'{prefix}: file already downloaded: {blob.name}')


def open_downloaded_blob(blob: Blob):
    return open(path.join(DOWNLOADS, blob.name), mode='r')


def push_inv_index(websites: Websites, db: Firestore, storage: Storage):
    blobs = storage.list_blobs(DEFAULT_BUCKET, prefix=INVINDEX_OUTPUT)
    inv_index_collection = db.collection('inv-index')

    for blob in blobs:
        if not VALID_PART.match(blob.name):
            continue

        maybe_download_blob(blob, storage, prefix='inv-index')

        spinner = Halo(text=f'inv-index: processing blob: {blob.name}')
        spinner.start()

        with open_downloaded_blob(blob) as file:
            batch = AutoBatch(db)

            for line in file:
                line = line.replace('\n', '')
                key_value = line.split('\t')
                word, urls = key_value[0], key_value[1]

                if not VALID_WORD.match(word):
                    continue

                urls = urls.split('|')
                websites_ids = [get_website_id(
                    url, websites, create=True) for url in urls]

                word_doc = inv_index_collection.document(word)
                batch.create(word_doc, {'websites': ArrayUnion(websites_ids)})

            batch.finish()

        spinner.succeed(f'inv-index: successfully processed: {blob.name}')


def push_page_rank(websites: Websites, db: Firestore, storage: Storage):
    blobs = storage.list_blobs(DEFAULT_BUCKET, prefix=PAGERANK_OUTPUT)
    page_rank_collection = db.collection('page-rank')

    for blob in blobs:
        if not VALID_PART.match(blob.name):
            continue

        maybe_download_blob(blob, storage, prefix='page-rank')

        spinner = Halo(text=f'page-rank: processing blob: {blob.name}')
        spinner.start()
        with open_downloaded_blob(blob) as file:
            batch = AutoBatch(db)

            for line in file:
                line = line.replace('\n', '')
                key = line.split('\t')[0]
                url_and_pr = key.split('|')
                url = url_and_pr[0]

                website_id = get_website_id(url, websites, create=False)
                if website_id is not None:
                    pr = float(url_and_pr[1])
                    pr_doc = page_rank_collection.document(website_id)
                    batch.create(pr_doc, {'value': pr})

            batch.finish()

        spinner.succeed(f'page-rank: successfully processed: {blob.name}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build Firestore database')
    parser.add_argument('--project-id', type=str,
                        required=True, help='Project id')

    args = parser.parse_args()
    print(f'Project: {args.project_id}')

    db = Firestore(project=args.project_id)
    storage = Storage(project=args.project_id)
    websites = Websites(db)

    push_inv_index(websites, db, storage)
    push_page_rank(websites, db, storage)
    websites.finish()
