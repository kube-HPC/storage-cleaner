const storageManager = require('@hkube/storage-manager');
const resultsCleaner = require('./results-cleaner');
const tempCleaner = require('./temp-cleaner');
const crons = [];

class CleanerManager {
    init(config, log) {
        resultsCleaner.init(config.cleaners.results, log);
        tempCleaner.init(config.cleaners.temp, log);
        crons.push(resultsCleaner);
        crons.push(tempCleaner);
    }

    async start() {
        const indices = await storageManager.hkubeIndex.listPrefixes();
        for (const c of crons) { // eslint-disable-line
            const { expiredDates, jobsToDelete } = await c.getJobsToDelete(indices); // eslint-disable-line
            await c.clean({ expiredDates, jobsToDelete }); // eslint-disable-line
        }
    }
}
module.exports = new CleanerManager();
