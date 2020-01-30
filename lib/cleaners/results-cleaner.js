const storageManager = require('@hkube/storage-manager');
const BaseCleaner = require('./base-cleaner');
const promiseWrapper = require('./promise-wrapper');

class ResultsCleaner extends BaseCleaner {
    async clean({ indices, expiredIndices, jobsToDelete }) {
        try {
            this.log.info(`found ${jobsToDelete.length} expired objects from ${indices.length} indices`, { component: this.name });

            for (let jobId of jobsToDelete) { // eslint-disable-line
                const promiseArray = [];
                promiseArray.push(promiseWrapper(() => storageManager.hkube.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeMetadata.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeExecutions.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeResults.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeAlgoMetrics.delete({ jobId }))); // eslint-disable-line
                const results = await Promise.all(promiseArray); // eslint-disable-line
                this._handleErrors(results);
            }

            if (expiredIndices.length > 0) {
                this.log.info(`found ${expiredIndices.length} expired indices`, { component: this.name });
                const results = await this.deleteIndices(expiredIndices);
                this._handleErrors(results);
            }
        }
        catch (error) {
            this.log.error(error, { component: this.name });
        }
    }
}

module.exports = new ResultsCleaner();
