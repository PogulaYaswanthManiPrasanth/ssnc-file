const fs = require('fs');

const path = require('path');

const redis = require("com.redis");

const _domainConfig = {
    "outputFolder": "./wprm_submission",
    "report": {
        "submitterID": "93010",
        "reporterIMID": "INGS",
        "reportGroup": "000001"
    },
    "reportFmt": "json",
    "redis": {
        "retry": {
            "attempt": 100,
            "total_retry_time": 3600000
        },
        "url": "redis://172.24.16.101:6379/1",
        "outputQueue": "finra-submitter::packager::output"
    },
    "domainSftpConfigKey": "wedbush",
    "file_validation_required": false
}
const submission = [
    {
        bzip: "93010_WPRM_20221212_B1WGZMCH8P1R_OrderEvents_001111.json",
        meta: "93010_WPRM_20221212_B1WGZMCH8P1R_OrderEvents_001111.meta.json"
    }
    
]

async function main() {
    try {
        const redisClient = await initializeRedisClient(_domainConfig.redis)
        const outputFolder = _domainConfig.outputFolder
        const submissionObject = {}
        submissionObject['domainSftpConfigKey'] = _domainConfig.domainSftpConfigKey

        for(const oneSubmission of submission) {
            const fileName = oneSubmission.bzip;
            const reportPath = path.join(outputFolder, `${fileName}`);
            submissionObject['bzipFilePath'] = `${reportPath}.bz2`
            submissionObject['bzipFileName'] = `${fileName}.bz2`
            
            const mdFileName = oneSubmission.meta;
            const mdFilePath = path.join(outputFolder, mdFileName);
            submissionObject['metaFileName'] = mdFileName
            submissionObject['metaFilePath'] = mdFilePath
            console.log(submissionObject)
    
            redisClient.lpush(_domainConfig.redis.outputQueue, JSON.stringify(submissionObject), (_res1, _res2) => { 
                console.log(_res1)
                console.log(_res2)
            });
        }

    } catch (error) {
        console.log(error)
    }
}

async function initializeRedisClient(config) {

    try {
        console.log(JSON.stringify(config))
        console.log(`config : ${config}`)

        const redisClient = redis.createClient(config.url, {
            retry_strategy: (options) => {

                if (options.error && (options.error.code === 'ECONNREFUSED' || options.error.code === 'NR_CLOSED')) {
                    // Try reconnecting after 5 seconds
                    console.log('The server refused the connection. Retrying connection...');
                    return 5000;
                }
                if (options.total_retry_time > config.retry.total_retry_time) {
                    // End reconnecting after a specific timeout and flush all commands with an individual error
                    return new Error('Retry time exhausted');
                }
                if (options.attempt > config.retry.attempt) {
                    // End reconnecting with built in error
                    return undefined;
                }
                // reconnect after
                return Math.min(options.attempt * 100, 3000);
            }
        });

        await initializeRedis(redisClient);

        return redisClient

    }
    catch (err) {

        console.log(err);
        throw err

    }

}

/**
 * initialize redis client
 *
 */
function initializeRedis(redisClient) {

    return new Promise((resolve, reject) => {

        redisClient.on('ready', () => {

            console.log('Redis connection success Ingals ReportSubmissionService');

            resolve(true);

        });

        redisClient.on('end', () => {

            console.log('REDIS connection closed for Ingals ReportSubmissionService.');

            resolve(true);

        });

        redisClient.on('error', (_error) => {

            console.log('REDIS connection error for Ingals ReportSubmissionService.', _error);

            redisClient.quit();

            reject(_error);

        });

    })

}

main()