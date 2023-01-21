
const fs = require('fs');

const path = require('path');

const Client = require('ssh2-sftp-client');

const generate = require('com.small-uuid/generate')

const redis = require("com.redis");


async function uploadFileToCustomerDomain(_file, _domainConfig) {

    try {
        console.log('------', path.basename(_file))

        const redisClient = await initializeRedisClient(_domainConfig.opts.redis)

        const fileName = path.basename(_file)
        const cacheKeyToCheckSubmission = getCacheKeyFromFileName(fileName)
        const getPreviousSubmission = await getCacheValue(redisClient, cacheKeyToCheckSubmission)
        console.log('@@@', getPreviousSubmission)
        const submissionCacheObject = getPreviousSubmission ? getPreviousSubmission : {}
        if (!getPreviousSubmission) {
            submissionCacheObject['linkageKey'] = getUUID(12)
        }
        
        const outputFolder = _domainConfig.opts.outputFolder
        const { submitterID, reporterIMID, reportGroup } = _domainConfig.opts.report

        const reportFmt = 'json'
        const submissionObject = {}
        submissionObject['domainSftpConfigKey'] = _domainConfig.opts.domainSftpConfigKey

        if (!path.basename(_file).includes('meta')) {
            const linkageKey = submissionCacheObject['linkageKey']
            const fileName = `${submitterID}_${reporterIMID}_${getCurrentDate(new Date())}_${linkageKey}_OrderEvents_${reportGroup}.${reportFmt}`;
            const reportPath = path.join(outputFolder, `${fileName}`);
            fs.createReadStream(_file).pipe(fs.createWriteStream(`${fileName}.bz2`));
            submissionObject['bzipFilePath'] = `${reportPath}.bz2`
            submissionObject['bzipFileName'] = `${fileName}.bz2`
            submissionCacheObject['report'] = submissionObject
            setRedisCache(redisClient, cacheKeyToCheckSubmission, submissionCacheObject)
            console.log(submissionObject)
        } else {
            const linkageKey = submissionCacheObject['linkageKey']
            const mdFileName = `${submitterID}_${reporterIMID}_${getCurrentDate(new Date())}_${linkageKey}_OrderEvents_${reportGroup}.meta.${reportFmt}`;
            const mdFilePath = path.join(outputFolder, mdFileName);
            fs.createReadStream(_file).pipe(fs.createWriteStream(`${mdFileName}`));
            submissionObject['metaFileName'] = mdFileName
            submissionObject['metaFilePath'] = mdFilePath
            submissionCacheObject['meta'] = submissionObject
            setRedisCache(redisClient, cacheKeyToCheckSubmission, submissionCacheObject)
            console.log(submissionObject)
        }

        const sftp = new Client();

        // const _sftpConfig = {
        //     host: _domainConfig.opts.host,
        //     port: 22,
        //     username: _domainConfig.opts.username,
        //     privateKey: require('fs').readFileSync(_domainConfig.opts.key)
        // }
        // console.log(_sftpConfig)
        // await sftp.connect(_sftpConfig);

        // if (!path.basename(_file).includes('meta')) {
        //     await sftp.put(submissionObject['bzipFileName'], `${_domainConfig.opts.drop_location}/${path.basename(submissionObject['bzipFileName'])}`);
        // } else {
        //     await sftp.put(submissionObject['metaFileName'], `${_domainConfig.opts.drop_location}/${path.basename(submissionObject['metaFileName'])}`);
        // }
        console.log(submissionCacheObject)
        // console.log(...submissionCacheObject['report'], ...submissionCacheObject['meta'])


        console.log(`----Succcessfully uploaded to cutomer domain-----`, `${_domainConfig.opts.drop_location}`);
        if(submissionCacheObject['report'] && submissionCacheObject['meta']) {
            // console.log(...submissionCacheObject['report'], ...submissionCacheObject['meta'])
            // redisClient.lpush(_domainConfig.opts.redis.outputQueue, JSON.stringify({ ...submissionCacheObject['report'], ...submissionCacheObject['meta'] }), (_res1, _res2) => { });
        }

    } catch (err) {
        console.log(`Error inside uploadFileToCustomerDomainScript.js `, err);
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

/**
     * get current data
     * @returns
     * @memberof ReportPackagerService
     */
function getCurrentDate(_reportDate) {

    let today = _reportDate || new Date();

    return (today.toISOString().split("T")[0]).replace(/-/g, "");

}

/**
   * Get short UUID based on the length
   * @param {Number} _len
   * @memberof ReportPackagerService
   */

function getUUID(_len) {

    return generate('1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ', _len)

}

function getCacheKeyFromFileName(fileName) {
    const fileNameComponent = fileName.split('_')
    return `${fileNameComponent[0]}_${fileNameComponent[1]}`
}

function getCacheValue(redisClient, _key) {
    return new Promise((resolve, reject) => {
        redisClient.get(_key, (err, data) => {
            if (err) {
                reject(err)
            }
            resolve(JSON.parse(data))
        })
    })
}

function setRedisCache(redisClient, _key, _data) {
    redisClient.set(_key, JSON.stringify(_data), (_res1, _res2) => {
        return true;
    });
}



exports.scriptToRun = uploadFileToCustomerDomain;