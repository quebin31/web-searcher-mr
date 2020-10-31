import { Storage } from '@google-cloud/storage';
import { Firestore, FieldValue, CollectionReference } from '@google-cloud/firestore';

import estream from 'event-stream';
import yargs from 'yargs';

const DEFAULT_BUCKET = 'web-searcher-cloud1';
const INVINDEX_OUTPUT = 'inv-index-output';
const PAGERANK_OUTPUT = 'page-rank-output';
const OUT_PART_REGEX = /part-r-[0-9]*$/;

interface Map {
    [x: string]: string,
}

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

async function storeWebsite(websiteUrl: string, websites: Map, collection: CollectionReference) {
    const doc = await collection.add({
        'url': websiteUrl
    });

    websites[websiteUrl] = doc.id;
}

async function storeInvIndex(websites: Map, storage: Storage, firestore: Firestore) {
    const invIndexFiles = await storage.bucket(DEFAULT_BUCKET).getFiles({
        directory: INVINDEX_OUTPUT,
    });

    const websitesCollection = firestore.collection('websites');
    const invIndexCollection = firestore.collection('invIndex');

    for (const file of invIndexFiles[0]) {
        if (!OUT_PART_REGEX.test(file.name)) continue;

        const stream = await storage.bucket(DEFAULT_BUCKET).file(file.name).createReadStream();

        stream.pipe(estream.split()).pipe(estream.map(async (line: string) => {
            const keyValue = line.split('\t');
            const [word, urls] = [keyValue[0], keyValue[1]];

            const wordDoc = invIndexCollection.doc(word);

            for (const url of urls.split('|')) {
                const websiteStored = url in websites;
                if (!websiteStored) await storeWebsite(url, websites, websitesCollection);

                const websiteId = websites[url];
                wordDoc.set({
                    'websites': FieldValue.arrayUnion(websiteId),
                }, { merge: true });
            }

        }));
    }
}

async function storePageRank(websites: Map, storage: Storage, firestore: Firestore) {
    const pageRankFiles = await storage.bucket(DEFAULT_BUCKET).getFiles({
        directory: PAGERANK_OUTPUT
    });

    const websitesCollection = firestore.collection('websites');
    const pageRankCollection = firestore.collection('pageRank');

    for (const file of pageRankFiles[0]) {
        if (!OUT_PART_REGEX.test(file.name)) continue;

        console.log(`Processing file ${file.name}`);

        const stream = storage.bucket(DEFAULT_BUCKET).file(file.name).createReadStream();

        stream.pipe(estream.split()).pipe(estream.map(async (line: string) => {
            stream.pause();

            const key = line.split('\t')[0];

            const urlAndPageRank = key.split('|');
            const [url, pageRank] = [urlAndPageRank[0], +urlAndPageRank[1]];
            console.log(`${url}: ${pageRank}`);

            /*
            const websitedStored = url in websites;
            if (!websitedStored) await storeWebsite(url, websites, websitesCollection);

            const websiteId = websites[url];
            const pageRankDoc = pageRankCollection.doc(websiteId);
            pageRankDoc.set({ value: pageRank }, { merge: true });
            */

            stream.resume();
        }));
    }
}

async function main(args: Arguments) {
    const storage = new Storage({
        projectId: args.projectId,
    });

    const firestore = new Firestore({
        projectId: args.projectId,
    });

    const websites = await getStoredWebsites(firestore);

    // console.log('Storing InvertedIndex output values');
    // await storeInvIndex(websites, storage, firestore);

    console.log('Storing PageRank output values');
    await storePageRank(websites, storage, firestore);
}

interface Arguments {
    [x: string]: unknown,
    projectId: string
}

const args: Arguments = yargs(process.argv.slice(2)).options({
    projectId: { type: 'string', alias: 'p', demandOption: true },
}).argv;

await main(args);