const moment = require('moment');
const promiseWrapper = require('./promise-wrapper');
const PATH_PATTERN = /(?<bucket>hkube-index)\/(?<date>\d{4}-\d{2}-\d{2})\/(?<jobId>.*)/; // eslint-disable-line

class BaseCleaner {
    init({ objectExpiration, name }, storageManager, log) {
        this.storageManager = storageManager;
        this.objExpiration = objectExpiration;
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
        const expiredIndices = indices.filter(i => this.isExpired(i));
        const jobsToDelete = [];
        const datesAndJobs = await Promise.all(expiredIndices.map(date => this.storageManager.hkubeIndex.list({ date })));
        datesAndJobs.forEach((date) => {
            date.forEach((job) => {
                const parsedPath = job.path.match(PATH_PATTERN);
                jobsToDelete.push(parsedPath.groups.jobId);
            });
        });
        return { expiredIndices, jobsToDelete };
    }

    async deleteIndices(indices) {
        return Promise.all(indices.map(date => promiseWrapper(() => this.storageManager.hkubeIndex.delete({ date }))));
    }

    _handleErrors(results) {
        const errors = results.filter(r => r instanceof Error);
        if (errors.length) {
            this.log.info(`failed to delete ${errors.length} objects`, { component: this.name });
            errors.forEach((error) => {
                this.log.info(`failed to delete ${error.message}`, { component: this.name });
            });
        }
    }
}

module.exports = BaseCleaner;
