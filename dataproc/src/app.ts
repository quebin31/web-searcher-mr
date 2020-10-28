import { ClusterControllerClient, JobControllerClient } from '@google-cloud/dataproc';
import { Storage } from '@google-cloud/storage';

const DEFAULT_BUCKET = 'web-searcher-unsa-1';
const WEB_OFFLINE = 'web-offline';
const INVINDEX_OUTPUT = 'inv-index-output';
const PAGERANK_TEMP = 'page-rank-temp';
const PAGERANK_OUTPUT = 'page-rank-output';

interface Options {
    projectId: string,
    region: string,
    clusterName: string,
    pageRankIterations: string,
    timeout: number,
}

function sleep(milliseconds: number) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

async function waitForJob(options: Options, jobClient: any, jobResp: any): Promise<string> {
    const terminalStates = new Set(['DONE', 'ERROR', 'CANCELLED']);
    const start = (new Date()).getMilliseconds();

    const jobReq = {
        projectId: options.projectId,
        region: options.region,
        jobId: jobResp.reference.jobId,
    };

    while (!terminalStates.has(jobResp.status.state)) {
        let now = (new Date()).getMilliseconds();
        if (now - options.timeout > start) {
            await jobClient.cancelJob(jobReq);

            console.log(`Job ${jobResp.reference.jobId} timed out after threshold of ${options.timeout / 60 * 1000} minutes`);
        }

        await sleep(1000);
        [jobResp] = await jobClient.getJob(jobReq);
    }

    return jobResp.status.state;
}

async function deleteCluster(options: Options, clusterClient: any) {
    const [deleteOperation] = await clusterClient.deleteCluster({
        projectId: options.projectId,
        region: options.region,
        clusterName: options.clusterName,
    });

    console.log(`Deleting cluster`);
    await deleteOperation.promise();

    console.log(`Cluster deleted succesfully`);
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
        console.log(`Creating bucket ${configClusterBucket} for this cluster`);
        await storage.createBucket(configClusterBucket, {
            location: options.region,
            regional: true,
            standard: true,
        });
    }


    const [createOperation] = await clusterClient.createCluster({
        projectId: options.projectId,
        region: options.region,
        cluster: {
            clusterName: options.clusterName,
            config: {
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
                    numInstances: 2,
                    machineTypeUri: 'n1-standard-1',
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

    console.log('Running InvertedIndex job ...');
    const invIndexStatus = await waitForJob(options, jobClient, invIndexJobResp);
    if (invIndexStatus !== 'DONE') {
        console.log('InvertedIndex job didn\'t finish succesfully');
        await deleteCluster(options, clusterClient);
        return;
    }

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
                    `gs://${DEFAULT_BUCKET}/${WEB_OFFLINE}}]`,
                    `gs://${DEFAULT_BUCKET}/${PAGERANK_TEMP}`,
                    `gs://${DEFAULT_BUCKET}/${PAGERANK_OUTPUT}`,
                    options.pageRankIterations.toString(),
                ]
            }
        }
    });

    console.log('Running PageRank job ...');
    const pageRankStatus = await waitForJob(options, jobClient, pageRankJobResp);
    if (pageRankStatus !== 'DONE') {
        console.log('PageRank job didn\'t finish succesfully');
        await deleteCluster(options, clusterClient);
        return;
    }


    const invIndexOutput = await storage
        .bucket(DEFAULT_BUCKET)
        .file(`${INVINDEX_OUTPUT}/part-r-00000`)
        .download();

    console.log(invIndexOutput);

    await deleteCluster(options, clusterClient);
}

const args = process.argv.slice(2);

if (args.length >= 4) {
    let timeout = (args.length === 5) ? +args[4] : 20;
    await main({
        projectId: args[0],
        region: args[1],
        clusterName: args[2],
        pageRankIterations: args[3],
        timeout: timeout * 60 * 1000,
    });
} else {
    console.log('Error: unexpected number of params');
    console.log('Usage: ... <projectId> <region> <clusterName> <pagerank-iterations> <timeout?>');
}
