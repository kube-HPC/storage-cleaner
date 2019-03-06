const storageManager = require('@hkube/storage-manager');
const BaseCleaner = require('./base-cleaner');
const PATH_PATTERN = /(?<bucket>hkube-index)\/(?<date>\d{4}-\d{2}-\d{2})\/(?<jobId>.*)/; // eslint-disable-line

class ResultsCleaner extends BaseCleaner {
    async clean() {
        try {
            const indexes = await storageManager.hkubeIndex.listPrefixes();
            const expiredDates = indexes.filter(i => super.isExpired(i));

            const jobsToDelete = [];
            const deletePromise = [];
            const datesAndJobs = await Promise.all(expiredDates.map(date => storageManager.hkubeIndex.list({ date })));
            datesAndJobs.forEach((date) => {
                date.forEach((job) => {
                    const parsedPath = job.path.match(PATH_PATTERN);
                    jobsToDelete.push(parsedPath.groups.jobId);
                });
            });

            this.log.info(`result-cleaner:found ${jobsToDelete.length} expired temp objects`);
            jobsToDelete.forEach((jobId) => {
                deletePromise.push(storageManager.hkube.delete({ jobId }));
                deletePromise.push(storageManager.hkubeMetadata.delete({ jobId }));
                deletePromise.push(storageManager.hkubeExecutions.delete({ jobId }));
                deletePromise.push(storageManager.hkubeResults.delete({ jobId }));
            });
            const results = await Promise.all(deletePromise);
            const failedResults = results.filter(result => (result instanceof Error));
            this.log.info(`result-cleaner:failed to delete ${failedResults.length}`);

            this.log.info(`result-cleaner:found ${expiredDates.length} expired indexes`);
            const indexesToDeletePromies = [];
            expiredDates.forEach((date) => {
                indexesToDeletePromies.push(storageManager.hkubeIndex.delete({ date }));
            });
            const resultsIndexes = await Promise.all(indexesToDeletePromies);
            const failedResultsIndexes = resultsIndexes.filter(result => (result instanceof Error));
            this.log.info(`result-cleaner:failed to delete indexes:${failedResultsIndexes.length}`);
        }
        catch (error) {
            this.log.error(error);
        }
    }
}

module.exports = new ResultsCleaner();
