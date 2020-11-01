import { Firestore, FieldValue, CollectionReference } from '@google-cloud/firestore';
import { Storage, File } from '@google-cloud/storage';
import { customAlphabet } from 'nanoid';

import estream from 'event-stream';
import yargs from 'yargs';

const DEFAULT_BUCKET = 'web-searcher-cloud1';
const INVINDEX_OUTPUT = 'inv-index-out-test';
const PAGERANK_OUTPUT = 'page-rank-out-test';
const OUT_PART_REGEX = /part-r-[0-9]*$/;
const VALID_WORD_REGEX = /[a-zA-Z0-9]+/;

interface Map {
    [x: string]: string,
}

interface Arguments {
    [x: string]: unknown,
    projectId: string
}

interface Files {
    invIndexFiles: File[],
    pageRankFiles: File[],
}

const generateId = customAlphabet('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 14);

async function getStoredWebsites(firestore: Firestore): Promise<Map> {
    const query = await firestore.collection('websites').get();

    const websites: Map = {};
    for (const doc of query.docs) {
        const websiteId = doc.id;
        const websiteUrl = doc.data().url;

        websites[websiteUrl] = websiteId;
    }

    return websites;
}

async function maybeStoreWebsite(websiteUrl: string, websites: Map, collection: CollectionReference) {
    if (websiteUrl in websites) {
        return;
    }

    const id = generateId();
    await collection.doc(id).set({ 'url': websiteUrl });
    websites[websiteUrl] = id;
}

function invIndexStep(files: Files, websites: Map, firestore: Firestore, storage: Storage) {
    const file = files.invIndexFiles.shift();
    if (file === undefined) {
        pageRankStep(files, websites, firestore, storage);
        return;
    }

    if (!OUT_PART_REGEX.test(file.name)) {
        invIndexStep(files, websites, firestore, storage);
        return;
    }

    console.log(`inv-index: processing file: ${file.name}`);

    const websitesCollection = firestore.collection('websites');
    const invIndexCollection = firestore.collection('inv-index');

    const stream = storage.bucket(DEFAULT_BUCKET).file(file.name).createReadStream();

    stream
        .on('end', () => invIndexStep(files, websites, firestore, storage))
        .pipe(estream.split())
        .pipe(estream.map(async (line: string) => {
            stream.pause();

            const keyValue = line.split('\t');
            const [word, urls] = [keyValue[0], keyValue[1]];

            if (!VALID_WORD_REGEX.test(word)) {
                stream.resume();
                return;
            }

            const wordDoc = invIndexCollection.doc(word);

            let batch = firestore.batch();
            let writes = 0;
            for (const url of urls.split('|')) {
                await maybeStoreWebsite(url, websites, websitesCollection);
                const websiteId = websites[url];

                batch.set(wordDoc, { 'websites': FieldValue.arrayUnion(websiteId) }, { merge: true });
                writes += 1;

                if (writes === 500) {
                    await batch.commit();
                    writes = 0;
                    batch = firestore.batch();
                }
            }

            await batch.commit();
            stream.resume();
        }));
}

function pageRankStep(files: Files, websites: Map, firestore: Firestore, storage: Storage) {
    const file = files.pageRankFiles.shift();
    if (file === undefined) return;

    if (!OUT_PART_REGEX.test(file.name)) {
        pageRankStep(files, websites, firestore, storage);
        return;
    }

    console.log(`page-rank: processing file: ${file.name}`);

    const websitesCollection = firestore.collection('websites');
    const pageRankCollection = firestore.collection('page-rank');

    const stream = storage.bucket(DEFAULT_BUCKET).file(file.name).createReadStream();

    stream
        .on('end', () => pageRankStep(files, websites, firestore, storage))
        .pipe(estream.split())
        .pipe(estream.map(async (line: string) => {
            stream.pause();

            const key = line.split('\t')[0];

            const urlAndPageRank = key.split('|');
            const [url, pageRank] = [urlAndPageRank[0], +urlAndPageRank[1]];

            const websitedStored = url in websites;
            if (!websitedStored) await maybeStoreWebsite(url, websites, websitesCollection);

            const websiteId = websites[url];
            const pageRankDoc = pageRankCollection.doc(websiteId);
            pageRankDoc.set({ value: pageRank }, { merge: true });

            stream.resume();
        }));
}

async function main(args: Arguments) {
    const storage = new Storage({ projectId: args.projectId });
    const firestore = new Firestore({ projectId: args.projectId });
    const websites = await getStoredWebsites(firestore);

    const invIndexFilesResponse = await storage.bucket(DEFAULT_BUCKET).getFiles({
        directory: INVINDEX_OUTPUT,
    });

    const pageRankFilesResponse = await storage.bucket(DEFAULT_BUCKET).getFiles({
        directory: PAGERANK_OUTPUT
    });

    const files: Files = {
        invIndexFiles: invIndexFilesResponse[0],
        pageRankFiles: pageRankFilesResponse[0]
    };

    invIndexStep(files, websites, firestore, storage);
}

await main(
    yargs(process.argv.slice(2))
        .options({
            projectId: { type: 'string', alias: 'p', demandOption: true },
        })
        .argv
);