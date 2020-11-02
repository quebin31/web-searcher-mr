import { Firestore, FieldValue, CollectionReference } from '@google-cloud/firestore';
import { Storage, File } from '@google-cloud/storage';
import { customAlphabet } from 'nanoid';
import { Mutex } from 'async-mutex';

import es from 'event-stream';
import yargs from 'yargs';

const DEFAULT_BUCKET = 'web-searcher-cloud1';
const INVINDEX_OUTPUT = 'inv-index-output';
const PAGERANK_OUTPUT = 'page-rank-output';
const OUT_PART_REGEX = /part-r-[0-9]*$/;
const VALID_WORD_REGEX = /[a-zA-Z0-9][a-zA-Z0-9-]*/;

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

const mutex = new Mutex();
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

async function getWebsiteId(websiteUrl: string, websites: Map, collection: CollectionReference, options: { create: boolean }): Promise<string | undefined> {
    const release = await mutex.acquire();
    let websiteId;

    try {
        if (websiteUrl in websites) {
            websiteId = websites[websiteUrl];
        } else if (options.create) {
            websiteId = generateId();
            await collection.doc(websiteId).set({ 'url': websiteUrl });
            websites[websiteUrl] = websiteId;
        }
    } catch (e) {
        console.log(`Error getting/creating website id for ${websiteUrl} (${e})`);
        throw e;
    } finally {
        release();
    }

    return websiteId;
}

function invIndexStep(files: Files, websites: Map, firestore: Firestore, storage: Storage) {
    console.log(`inv-index: files remaining: ${files.invIndexFiles.length}`);
    const file = files.invIndexFiles.shift();
    if (file === undefined) {
        console.log('inv-index: stepping into pagerank');
        pageRankStep(files, websites, firestore, storage);
        return;
    }

    if (!OUT_PART_REGEX.test(file.name)) {
        console.log(`inv-index: skipping file: ${file.name}`);
        invIndexStep(files, websites, firestore, storage);
        return;
    }

    console.log(`inv-index: processing file: ${file.name}`);

    const websitesCollection = firestore.collection('websites');
    const invIndexCollection = firestore.collection('inv-index');

    const fileStream = storage.bucket(DEFAULT_BUCKET).file(file.name).createReadStream();


    fileStream
        .on('error', (e) => console.log(`inv-index: error stream: ${e}`))
        .on('close', () => {
            console.log(`inv-index: closed stream: ${file.name}`);
            invIndexStep(files, websites, firestore, storage);
        })
        .pipe(es.split())
        .pipe(es.mapSync(async (line: string) => {
            fileStream.pause();

            const keyValue = line.split('\t');
            const [word, urls] = [keyValue[0], keyValue[1]];

            if (!VALID_WORD_REGEX.test(word)) {
                fileStream.resume();
                return;
            }

            console.log(`inv-index: processing word: ${word}`);

            const wordDoc = invIndexCollection.doc(word);

            let batch = firestore.batch();
            let writes = 0;
            for (const url of urls.split('|')) {
                const websiteId = await getWebsiteId(url, websites, websitesCollection, { create: true });

                batch.set(wordDoc, { 'websites': FieldValue.arrayUnion(websiteId) }, { merge: true });
                writes += 1;

                if (writes === 499) {
                    await batch.commit();
                    writes = 0;
                    batch = firestore.batch();
                }
            }

            await batch.commit();

            console.log(`inv-index: finished word: ${word}`);

            fileStream.resume();
        }));
}


function pageRankStep(files: Files, websites: Map, firestore: Firestore, storage: Storage) {
    console.log(`page-rank: files remaining: ${files.pageRankFiles.length}`);
    const file = files.pageRankFiles.shift();
    if (file === undefined) return;

    if (!OUT_PART_REGEX.test(file.name)) {
        console.log(`page-rank: skipping file: ${file.name}`);
        pageRankStep(files, websites, firestore, storage);
        return;
    }

    console.log(`page-rank: processing file: ${file.name}`);

    const websitesCollection = firestore.collection('websites');
    const pageRankCollection = firestore.collection('page-rank');

    const fileStream = storage.bucket(DEFAULT_BUCKET).file(file.name).createReadStream();

    fileStream
        .on('error', (e) => console.log(`page-rank: error stream: ${e}`))
        .on('close', () => {
            console.log(`page-rank: closed stream: ${file.name}`);
            pageRankStep(files, websites, firestore, storage);
        })
        .pipe(es.split())
        .pipe(es.mapSync(async (line: string) => {
            fileStream.pause();

            const key = line.split('\t')[0];

            const urlAndPageRank = key.split('|');
            const [url, pageRank] = [urlAndPageRank[0], +urlAndPageRank[1]];

            const websiteId = await getWebsiteId(url, websites, websitesCollection, { create: false });
            if (websiteId !== undefined) {
                console.log(`page-rank: processing url: ${url}`);
                const pageRankDoc = pageRankCollection.doc(websiteId);
                pageRankDoc.set({ value: pageRank }, { merge: true });
                console.log(`page-rank: finished url: ${url}`);
            }

            fileStream.resume();
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