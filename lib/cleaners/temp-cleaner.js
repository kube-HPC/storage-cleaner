const BaseCleaner = require('./base-cleaner');
const promiseWrapper = require('./promise-wrapper');

class TempCleaner extends BaseCleaner {
    async clean({ indices, jobsToDelete }) {
        try {
            this.log.info(`found ${jobsToDelete.length} expired objects from ${indices.length} indices`, { component: this.name });

            for (let jobId of jobsToDelete) { // eslint-disable-line
                const promiseArray = [];
                promiseArray.push(promiseWrapper(() => this.storageManager.hkube.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => this.storageManager.hkubeMetadata.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => this.storageManager.hkubeExecutions.delete({ jobId }))); // eslint-disable-line
                const results = await Promise.all(promiseArray); // eslint-disable-line
                this._handleErrors(results);
            }
        }
        catch (error) {
            this.log.error(error, { component: this.name });
        }
    }
}

module.exports = new TempCleaner();
