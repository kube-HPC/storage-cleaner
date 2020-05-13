
const resultsCleaner = require('./results-cleaner');
const tempCleaner = require('./temp-cleaner');
const crons = [];

class CleanerManager {
    init(storageManager, config, log) {
        this.storageManager = storageManager;
        resultsCleaner.init(config.cleaners.results, storageManager, log);
        tempCleaner.init(config.cleaners.temp, storageManager, log);
        crons.push(resultsCleaner);
        crons.push(tempCleaner);
    }

    async start() {
        const indices = await this.storageManager.hkubeIndex.listPrefixes();
        for (const c of crons) { // eslint-disable-line
            const { expiredIndices, jobsToDelete } = await c.getJobsToDelete(indices); // eslint-disable-line
            await c.clean({ indices, expiredIndices, jobsToDelete }); // eslint-disable-line
        }
    }
}
module.exports = new CleanerManager();
