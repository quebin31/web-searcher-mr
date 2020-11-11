import { Request, Response } from 'express';
import { Firestore } from '@google-cloud/firestore';

const projectId = 'web-searcher-293217';
const firestore = new Firestore({ projectId });

interface QueryData {
    webId: string,
    count: number,
}

interface SortData {
    webId: string,
    value: number,
    pagerank: number,
}

exports.query = async (req: Request, res: Response) => {
    // Query parameters
    const word = req.query.word;
    const limit = Number(req.query.limit);

    if (word === undefined) {
        res.status(200).json({ results: [] });
        return;
    }

    const wordWebsitesSnaps = await firestore
        .collection('inv-index')
        .doc((word as string).toLowerCase())
        .collection('websites').get();

    if (wordWebsitesSnaps.empty) {
        res.status(200).json({ results: [] });
        return;
    }

    const allQueryData: QueryData[] = [];
    for (const wordWebsiteDoc of wordWebsitesSnaps.docs) {
        const webId = wordWebsiteDoc.id;
        const count = wordWebsiteDoc.data()?.count as number;

        allQueryData.push({
            webId,
            count,
        });
    }

    const pageRankDocs = allQueryData.map((data: QueryData) => firestore.collection('page-rank').doc(data.webId));
    const pageRankSnapshots = await firestore.getAll(...pageRankDocs);

    const allSortData: SortData[] = allQueryData.map((data: QueryData, idx) => {
        const count = (data.count > 20) ? 20 : data.count;
        const pagerank = pageRankSnapshots[idx].data()?.value;

        return {
            webId: data.webId,
            value: pagerank + (count / 20),
            pagerank,
        }
    });

    const endIdx = isNaN(limit) ? 10 : (limit === -1) ? undefined : limit;
    const sortedData = allSortData
        .sort((a, b) => b.value - a.value)
        .slice(0, endIdx);

    const sortedWebDocs = sortedData.map((sortData) => firestore.collection('websites').doc(sortData.webId));

    const sortedWebSnapshots = await firestore.getAll(...sortedWebDocs);
    const results = sortedWebSnapshots.map((snapshot, idx) => {
        return {
            url: snapshot.data()?.url,
            pagerank: sortedData[idx].pagerank,
        }
    });

    res.status(200).json({ results });
};