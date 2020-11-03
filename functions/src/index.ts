import { Request, Response } from 'express';
import { Firestore } from '@google-cloud/firestore';

const projectId = 'web-searcher-293217';
const firestore = new Firestore({ projectId });

exports.query = async (req: Request, res: Response) => {
    let results: string[] = [];
    const word = req.query.word;
    const limit = +(req.query.limit || '10');

    if (word !== undefined) {
        const wordDoc = await firestore.collection('inv-index').doc(req.query.word as string).get();
        const websites = wordDoc.data()?.websites;

        const pageRankDocs = websites.map((website: string) => firestore.collection('page-rank').doc(website));
        const pageRankSnapshots = await firestore.getAll(...pageRankDocs);

        const sorted = pageRankSnapshots.sort((a, b) => b.data()?.value - a.data()?.value).slice(0, limit);
        const webDocs = sorted.map((doc) => firestore.collection('websites').doc(doc.id));
        const webSnapshots = await firestore.getAll(...webDocs);

        results = webSnapshots.map((snapshot) => snapshot.data()?.url);
    }

    res.status(200).json({ results });
};