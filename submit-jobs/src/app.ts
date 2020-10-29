import { ClusterControllerClient, JobControllerClient } from '@google-cloud/dataproc';
import { Storage } from '@google-cloud/storage';

const DEFAULT_BUCKET = 'web-searcher-cloud1';
const WEB_OFFLINE = 'web-offline';
const INVINDEX_OUTPUT = 'inv-index-output';
const PAGERANK_TEMP = 'page-rank-temp';
const PAGERANK_OUTPUT = 'page-rank-output';

interface Options {
    projectId: string,
    region: string,
    clusterName: string,
    pageRankIterations: string,
    filter: String[],
}


async function main(options: Options) {
    console.log('Received the following options:', options);

    const clusterClient = new ClusterControllerClient({
        apiEndpoint: `${options.region}-dataproc.googleapis.com`,
        projectId: options.projectId,
    });

    const jobClient = new JobControllerClient({
        apiEndpoint: `${options.region}-dataproc.googleapis.com`,
        projectId: options.projectId,
    });

    const storage = new Storage();
    const configClusterBucket = `${options.clusterName}-config`;
    const buckets = await storage.getBuckets();
    if (!buckets[0].some((bucket) => bucket.name === configClusterBucket)) {
        console.log(`Creating config bucket ${configClusterBucket} for this cluster`);
        await storage.createBucket(configClusterBucket, {
            location: options.region,
            regional: true,
            standard: true,
        });
    }

    const [clusters] = await clusterClient.listClusters({
        projectId: options.projectId,
        region: options.region
    });

    if (!clusters.some((c) => c.clusterName == options.clusterName)) {
        const [createOperation] = await clusterClient.createCluster({
            projectId: options.projectId,
            region: options.region,
            cluster: {
                clusterName: options.clusterName,
                config: {
                    configBucket: configClusterBucket,
                    lifecycleConfig: {
                        idleDeleteTtl: {
                            seconds: 600,
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

        console.log(`Creating cluster: ${options.clusterName} ... (this may take some time)`);
        const [createResponse] = await createOperation.promise();
        console.log(`Cluster created successfully: ${createResponse.clusterName}`);
    }

    if (await storage.bucket(DEFAULT_BUCKET).file(INVINDEX_OUTPUT).exists()) {
        await storage.bucket(DEFAULT_BUCKET).deleteFiles({
            directory: INVINDEX_OUTPUT,
        });
    }

    if (await storage.bucket(DEFAULT_BUCKET).file(PAGERANK_OUTPUT).exists()) {
        await storage.bucket(DEFAULT_BUCKET).deleteFiles({
            directory: PAGERANK_OUTPUT
        })
    }

    if (!options.filter.some((f) => f === 'inv-index')) {
        console.log('Submitting InvertedIndex job ...');
        const [invIndexJobResp] = await jobClient.submitJob({
            projectId: options.projectId,
            region: options.region,
            job: {
                placement: {
                    clusterName: options.clusterName,
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
    }

    if (!options.filter.some((f) => f === 'page-rank')) {
        console.log('Submitting PageRank job ...');
        const [pageRankJobResp] = await jobClient.submitJob({
            projectId: options.projectId,
            region: options.region,
            job: {
                placement: {
                    clusterName: options.clusterName,
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
                        options.pageRankIterations.toString(),
                    ]
                }
            }
        });
        console.log(`Job submitted: ${pageRankJobResp.reference?.jobId}`);
    }
}

const args = process.argv.slice(2);
if (args.length >= 4) {
    await main({
        projectId: args[0],
        region: args[1],
        clusterName: args[2],
        pageRankIterations: args[3],
        filter: (args.length === 5) ? args[4].split(',') : []
    });
} else {
    console.log('Error: unexpected number of params');
    console.log('Usage: ... <projectId> <region> <clusterName> <pagerank-iterations> <filter>');
}
