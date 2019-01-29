const storageManager = require('@hkube/storage-manager');
const BaseCleaner = require('./base-cleaner');
const Logger = require('@hkube/logger');
const log = Logger.GetLogFromContainer();
const PATH_PATTERN = /(?<bucket>hkube-index)\/(?<date>\d{4}-\d{2}-\d{2})\/(?<jobId>.*)/; // eslint-disable-line

class TempCleaner extends BaseCleaner {
    async clean() {
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

        log.info(`temp-cleaner:found ${jobsToDelete.length} expired temp objects`);
        jobsToDelete.forEach((jobId) => {
            deletePromise.push(storageManager.hkube.delete({ jobId }));
            deletePromise.push(storageManager.hkubeMetadata.delete({ jobId }));
            deletePromise.push(storageManager.hkubeExecutions.delete({ jobId }));
        });
        await Promise.all(deletePromise);
    }
}

module.exports = new TempCleaner();
