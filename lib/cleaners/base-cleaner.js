const moment = require('moment');
const storageManager = require('@hkube/storage-manager');
const promiseWrapper = require('./promise-wrapper');
const PATH_PATTERN = /(?<bucket>hkube-index)\/(?<date>\d{4}-\d{2}-\d{2})\/(?<jobId>.*)/; // eslint-disable-line

class BaseCleaner {
    init({ objectExpiration, cronPattern, name }, log) {
        this.objExpiration = objectExpiration;
        this.cronPattern = cronPattern;
        this.name = name;
        this.log = log;
    }

    isExpired(date) {
        if (moment(date).isBefore(moment().subtract(this.objExpiration, 'days'))) {
            return true;
        }
        return false;
    }

    async getJobsToDelete(indices) {
        const expiredDates = indices.filter(i => this.isExpired(i));
        const jobsToDelete = [];
        const datesAndJobs = await Promise.all(expiredDates.map(date => storageManager.hkubeIndex.list({ date })));
        datesAndJobs.forEach((date) => {
            date.forEach((job) => {
                const parsedPath = job.path.match(PATH_PATTERN);
                jobsToDelete.push(parsedPath.groups.jobId);
            });
        });
        return { expiredDates, jobsToDelete };
    }

    async deleteIndices(indices) {
        const indicesToDeletePromises = [];
        indices.forEach((date) => {
            indicesToDeletePromises.push(promiseWrapper(() => storageManager.hkubeIndex.delete({ date })));
        });
        const results = await Promise.all(indicesToDeletePromises);
        return results;
    }

    _handleErrors(results) {
        const errors = results.filter(r => r instanceof Error);
        if (errors.length) {
            this.log.info(`${this.name}: failed to delete ${errors.length} objects`);
            errors.forEach((error) => {
                this.log.info(`${this.name}: failed to delete ${error.message}`);
            });
        }
    }
}

module.exports = BaseCleaner;
