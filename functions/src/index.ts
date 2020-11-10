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

        return {
            webId: data.webId,
            value: pageRankSnapshots[idx].data()?.value + (count / 20),
        }
    });

    const endIdx = isNaN(limit) ? 10 : (limit === -1) ? undefined : limit;
    const sortedWebDocs = allSortData
        .sort((a, b) => b.value - a.value)
        .slice(0, endIdx)
        .map((sortData) => firestore.collection('websites').doc(sortData.webId));

    const sortedWebSnapshots = await firestore.getAll(...sortedWebDocs);
    const results = sortedWebSnapshots.map((snapshot) => snapshot.data()?.url);
    res.status(200).json({ results });
};