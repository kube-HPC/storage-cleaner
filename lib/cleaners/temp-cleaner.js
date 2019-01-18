const storageManager = require('@hkube/storage-manager');
const BaseCleaner = require('./base-cleaner');
const Logger = require('@hkube/logger');
const log = Logger.GetLogFromContainer();
const DATE_INDEX_PATTERN = /(\d{4}-\d{2}-\d{2})\/(.*)/;

class TempCleaner extends BaseCleaner {
    async clean() {
        const indexes = await storageManager.hkubeIndex.list({ date: '' });
        const parsedJobs = indexes.map(d => d.path.match(DATE_INDEX_PATTERN));
        const datesToDelete = [];
        const jobsToDelete = [];
        const deletePromiess = [];

        parsedJobs.forEach((line) => {
            if (super.isExpired(line[1])) {
                datesToDelete.push(line[1]);
                jobsToDelete.push(line[2]);
            }
        });
        log.info(`found ${jobsToDelete.length} expired temp objects`);
        jobsToDelete.forEach((jobId) => {
            deletePromiess.push(storageManager.hkube.delete({ jobId }));
            deletePromiess.push(storageManager.hkubeMetadata.delete({ jobId }));
            deletePromiess.push(storageManager.hkubeExecutions.delete({ jobId }));
        });
        await Promise.all(deletePromiess);
    }
}

module.exports = new TempCleaner();
