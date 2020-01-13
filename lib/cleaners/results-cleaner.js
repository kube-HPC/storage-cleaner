const storageManager = require('@hkube/storage-manager');
const BaseCleaner = require('./base-cleaner');
const promiseWrapper = require('./promise-wrapper');

class ResultsCleaner extends BaseCleaner {
    async clean({ expiredDates, jobsToDelete }) {
        try {
            this.log.info(`${this.name}: found ${jobsToDelete.length} expired objects`);

            for (let jobId of jobsToDelete) { // eslint-disable-line
                const promiseArray = [];
                promiseArray.push(promiseWrapper(() => storageManager.hkube.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeMetadata.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeExecutions.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeResults.delete({ jobId }))); // eslint-disable-line
                const results = await Promise.all(promiseArray); // eslint-disable-line
                this._handleErrors(results);
            }

            if (expiredDates.length > 0) {
                this.log.info(`${this.name}: found ${expiredDates.length} expired indices`);
                const results = await this.deleteIndices(expiredDates);
                this._handleErrors(results);
            }
        }
        catch (error) {
            this.log.error(error);
        }
    }
}

module.exports = new ResultsCleaner();
