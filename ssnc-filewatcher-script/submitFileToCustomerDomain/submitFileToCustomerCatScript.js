
const fs = require('fs');

const path = require('path');

const Client = require('ssh2-sftp-client');

const generate = require('com.small-uuid/generate')

const redis = require("com.redis");

const linkageKey = getUUID(12)

async function uploadFileToCustomerDomain(_file, _domainConfig) {

    try {

        if (!_file || !_domainConfig) {
            throw new Error(`Required parameters _file,_domainConfig are missing`);
        }

        if (_domainConfig.opts.hosts.length) {
            const submissionFilePathArray = []

            for (const domainConfig of _domainConfig.opts.hosts) {

                try {

                    const backUpPath = domainConfig.backup_dir;

                    if (!backUpPath) {
                        throw new Error('Backup folder path is required');
                    }

                    if (!fs.existsSync(backUpPath)) {
                        throw new Error('Backup path is not created on the current machine');
                    }

                    const todayDate = new Date().toISOString().split('T')[0];

                    console.log('Date of submission', todayDate)
                    console.log('1. file name', path.basename(_file))
                    console.log('2. linkageKey', linkageKey)

                    const redisClient = await initializeRedisClient(domainConfig.redis)

                    const outputFolder = domainConfig.outputFolder
                    const { submitterID, reporterIMID, reportGroup } = domainConfig.report

                    const reportFmt = 'json'
                    const submissionObject = {}
                    submissionObject['domainSftpConfigKey'] = domainConfig.domainSftpConfigKey

                    if (path.basename(_file).includes('.json.bz2')) {
                        const fileName = `${submitterID}_${reporterIMID}_${getCurrentDate(new Date())}_${linkageKey}_OrderEvents_${reportGroup}.${reportFmt}`;
                        const reportPath = path.join(outputFolder, `${fileName}`);
                        fs.createReadStream(_file).pipe(fs.createWriteStream(`${fileName}.bz2`));
                        submissionObject['bzipFilePath'] = `${reportPath}.bz2`
                        submissionObject['bzipFileName'] = `${fileName}.bz2`

                    } else if (path.basename(_file).includes('.meta.json')) {
                        const mdFileName = `${submitterID}_${reporterIMID}_${getCurrentDate(new Date())}_${linkageKey}_OrderEvents_${reportGroup}.meta.${reportFmt}`;
                        const mdFilePath = path.join(outputFolder, mdFileName);
                        fs.createReadStream(_file).pipe(fs.createWriteStream(`${mdFileName}`));
                        submissionObject['metaFileName'] = mdFileName
                        submissionObject['metaFilePath'] = mdFilePath
                    }

                    const sftp = new Client();

                    const _sftpConfig = {
                        host: domainConfig.sftp.host,
                        port: 22,
                        username: domainConfig.sftp.username,
                        privateKey: require('fs').readFileSync(domainConfig.sftp.key)
                    }

                    await sftp.connect(_sftpConfig);

                    if (path.basename(_file).includes('.json.bz2')) {
                        await sftp.put(submissionObject['bzipFileName'], `${domainConfig.sftp.drop_location}/${path.basename(submissionObject['bzipFileName'])}`);
                        setTimeout(() => {
                            const submissionFilePath = path.join(process.cwd(), submissionObject['bzipFileName'])
                            fs.copyFileSync(submissionFilePath, backUpPath + `/${todayDate}_${submissionObject['bzipFileName']}`);                         //copying file to backup location after sending it to domain location
                            submissionFilePathArray.push(submissionFilePath)
                            console.log(`Copied "${path.basename(_file)}" to backup Path "${backUpPath}" successfully`);
                        }, 10000)
                    } else if (path.basename(_file).includes('.meta.json')) {
                        await sftp.put(submissionObject['metaFileName'], `${domainConfig.sftp.drop_location}/${path.basename(submissionObject['metaFileName'])}`);
                        setTimeout(() => {
                            const submissionFilePath = path.join(process.cwd(), submissionObject['metaFileName'])
                            fs.copyFileSync(submissionFilePath, backUpPath + `/${todayDate}_${submissionObject['metaFileName']}`);                        //copying file to backup location after sending it to domain location
                            submissionFilePathArray.push(submissionFilePath)
                            console.log(`Copied "${path.basename(_file)}" to backup Path "${backUpPath}" successfully`);
                        }, 10000)
                    }
                    console.log('3. submission', submissionObject)

                    redisClient.lpush(domainConfig.redis.outputQueue, JSON.stringify(submissionObject), (_res1, _res2) => {
                        console.log(_res1)
                        console.log(_res2)
                    });
                    console.log(`----Succcessfully submitted to customer cat-----`, `${domainConfig.sftp.drop_location}`);

                } catch (error) {
                    console.log('Error while submission, let us try another submission if pending', error)
                }

            }
            setTimeout(() => {
                for (const filePath of [...new Set(submissionFilePathArray)]) {
                    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);                        //delete file from script folders
                }
            }, 20000)

        }

    } catch (err) {
        console.log(`Error inside uploadFileToCustomerDomainScript.js `, err);
        throw error
    }

}


async function initializeRedisClient(config) {

    try {
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

exports.scriptToRun = uploadFileToCustomerDomain;