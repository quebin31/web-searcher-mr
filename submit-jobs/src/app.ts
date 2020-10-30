import { ClusterControllerClient, JobControllerClient } from '@google-cloud/dataproc';
import { Storage } from '@google-cloud/storage';
import yargs from 'yargs';

const DEFAULT_BUCKET = 'web-searcher-cloud1';
const WEB_OFFLINE = 'web-offline';
const INVINDEX_OUTPUT = 'inv-index-output';
const PAGERANK_TEMP = 'page-rank-temp';
const PAGERANK_OUTPUT = 'page-rank-output';

async function main(args: Arguments) {
    console.log('Received the following arguments:', args);

    const clusterClient = new ClusterControllerClient({
        apiEndpoint: `${args.region}-dataproc.googleapis.com`,
        projectId: args.projectId,
    });

    const jobClient = new JobControllerClient({
        apiEndpoint: `${args.region}-dataproc.googleapis.com`,
        projectId: args.projectId,
    });

    const storage = new Storage();
    const configClusterBucket = `${args.clusterName}-config`;
    const buckets = await storage.getBuckets();
    if (!buckets[0].some((bucket) => bucket.name === configClusterBucket)) {
        console.log(`Creating config bucket ${configClusterBucket} for this cluster`);
        await storage.createBucket(configClusterBucket, {
            location: args.region,
            regional: true,
            standard: true,
        });
    }

    const [clusters] = await clusterClient.listClusters({
        projectId: args.projectId,
        region: args.region
    });

    if (!clusters.some((c) => c.clusterName == args.clusterName)) {
        const [createOperation] = await clusterClient.createCluster({
            projectId: args.projectId,
            region: args.region,
            cluster: {
                clusterName: args.clusterName,
                config: {
                    configBucket: configClusterBucket,
                    lifecycleConfig: {
                        idleDeleteTtl: {
                            seconds: 15 * 60,
                        },
                    },
                    softwareConfig: {
                        imageVersion: 'preview-debian10',
                    },
                    masterConfig: {
                        numInstances: 1,
                        machineTypeUri: 'n1-standard-1',
                        diskConfig: {
                            bootDiskType: 'pd-standard',
                            bootDiskSizeGb: 500,
                        }
                    },
                    workerConfig: {
                        numInstances: 3,
                        machineTypeUri: 'n1-standard-2',
                        diskConfig: {
                            bootDiskType: 'pd-standard',
                            bootDiskSizeGb: 500,
                        }
                    },
                },
            },
        });

        console.log(`Creating cluster: ${args.clusterName} ... (this may take some time)`);
        const [createResponse] = await createOperation.promise();
        console.log(`Cluster created successfully: ${createResponse.clusterName}`);
    } else {
        console.log(`Cluster ${args.clusterName} already exists`);
    }

    const files = await storage.bucket(DEFAULT_BUCKET).getFiles();
    const pageRankFiltered = args.filter.some((f) => f === 'page-rank');
    const invIndexFiltered = args.filter.some((f) => f === 'inv-index');

    if (!invIndexFiltered && files[0].some((f) => f.name === INVINDEX_OUTPUT)) {
        console.log('InvertedIndex output exists');
        if (args.force) {
            console.log('Deleting InvertedIndex output');
            await storage.bucket(DEFAULT_BUCKET).deleteFiles({
                directory: INVINDEX_OUTPUT,
            });
        } else {
            console.log('If you want to remove them run with \'--force\'');
            return;
        }
    }

    if (!pageRankFiltered && files[0].some((f) => f.name === PAGERANK_TEMP)) {
        console.log('PageRank temp output exists');
        if (args.force) {
            console.log('Deleting PageRank temp output');
            await storage.bucket(DEFAULT_BUCKET).deleteFiles({
                directory: PAGERANK_TEMP
            })
        } else {
            console.log('If you want to remove them run with \'--force\'');
            return;
        }
    }

    if (!pageRankFiltered && files[0].some((f) => f.name === PAGERANK_OUTPUT)) {
        console.log('PageRank output exists');
        if (args.force) {
            console.log('Deleting PageRank output');
            await storage.bucket(DEFAULT_BUCKET).deleteFiles({
                directory: PAGERANK_OUTPUT
            })
        } else {
            console.log('If you want to remove them run with \'--force\'');
            return;
        }
    }

    if (!invIndexFiltered) {
        console.log('Submitting InvertedIndex job ...');
        const [invIndexJobResp] = await jobClient.submitJob({
            projectId: args.projectId,
            region: args.region,
            job: {
                placement: {
                    clusterName: args.clusterName,
                },
                hadoopJob: {
                    mainClass: 'InvertedIndex',
                    jarFileUris: [
                        `gs://${DEFAULT_BUCKET}/inv-index.jar`
                    ],
                    args: [
                        `gs://${DEFAULT_BUCKET}/${WEB_OFFLINE}`,
                        `gs://${DEFAULT_BUCKET}/${INVINDEX_OUTPUT}`
                    ]
                },
            },
        });
        console.log(`Job submitted: ${invIndexJobResp.reference?.jobId}`);
    } else {
        console.log('InvertedIndex job was filtered');
    }

    if (!pageRankFiltered) {
        console.log('Submitting PageRank job ...');
        const [pageRankJobResp] = await jobClient.submitJob({
            projectId: args.projectId,
            region: args.region,
            job: {
                placement: {
                    clusterName: args.clusterName,
                },
                hadoopJob: {
                    mainClass: 'PageRank',
                    jarFileUris: [
                        `gs://${DEFAULT_BUCKET}/page-rank.jar`
                    ],
                    args: [
                        `gs://${DEFAULT_BUCKET}/${WEB_OFFLINE}`,
                        `gs://${DEFAULT_BUCKET}/${PAGERANK_TEMP}`,
                        `gs://${DEFAULT_BUCKET}/${PAGERANK_OUTPUT}`,
                        args.pageRankIters.toString(),
                    ]
                }
            }
        });
        console.log(`Job submitted: ${pageRankJobResp.reference?.jobId}`);
    } else {
        console.log('PageRank job was filtered');
    }
}

interface Arguments {
    [x: string]: unknown,
    projectId: string,
    region: string,
    clusterName: string,
    pageRankIters: number,
    filter: string[],
    force: boolean,
}

const args: Arguments = yargs(process.argv.slice(2)).options({
    force: { type: 'boolean', default: false },
    projectId: { type: 'string', alias: 'p', demandOption: true },
    region: { type: 'string', alias: 'r', demandOption: true },
    clusterName: { type: 'string', alias: 'c', demandOption: true },
    pageRankIters: { type: 'number', alias: 'i', default: 10 },
    filter: { type: 'array', alias: 'f', default: [] },
}).argv;

await main(args);
