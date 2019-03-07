const storageManager = require('@hkube/storage-manager');
const BaseCleaner = require('./base-cleaner');
const promiseWrapper = require('./promise-wrapper');
const PATH_PATTERN = /(?<bucket>hkube-index)\/(?<date>\d{4}-\d{2}-\d{2})\/(?<jobId>.*)/; // eslint-disable-line

class TempCleaner extends BaseCleaner {
    async clean() {
        try {
            const indexes = await storageManager.hkubeIndex.listPrefixes();
            const expiredDates = indexes.filter(i => super.isExpired(i));

            const jobsToDelete = [];
            const datesAndJobs = await Promise.all(expiredDates.map(date => storageManager.hkubeIndex.list({ date })));
            datesAndJobs.forEach((date) => {
                date.forEach((job) => {
                    const parsedPath = job.path.match(PATH_PATTERN);
                    jobsToDelete.push(parsedPath.groups.jobId);
                });
            });

            this.log.info(`temp-cleaner:found ${jobsToDelete.length} expired temp objects`);

            for (let jobId of jobsToDelete) { // eslint-disable-line
                const promiseArray = [];
                promiseArray.push(promiseWrapper(() => storageManager.hkube.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeMetadata.delete({ jobId }))); // eslint-disable-line
                promiseArray.push(promiseWrapper(() => storageManager.hkubeExecutions.delete({ jobId }))); // eslint-disable-line
                const results = await Promise.all(promiseArray); // eslint-disable-line
                this._handleErrors(results);
            }
        }
        catch (error) {
            this.log.error(error);
        }
    }

    _handleErrors(results) {
        const errors = results.filter(r => r instanceof Error);
        if (errors.length) {
            this.log.info(`temp-cleaner:failed to delete ${errors.length} objects`);
            errors.forEach((error) => {
                this.log.info(`temp-cleaner:failed to delete ${error.message}`);
            });
        }
    }
}

module.exports = new TempCleaner();
