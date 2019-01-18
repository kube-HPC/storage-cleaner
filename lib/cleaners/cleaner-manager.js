const resultsCleaner = require('./results-cleaner');
const tempCleaner = require('./temp-cleaner');
const crons = [];

class CleanerManager {
    init(config) {
        resultsCleaner.init(config.cleaners.results);
        tempCleaner.init(config.cleaners.temp);
        crons.push(resultsCleaner);
        crons.push(tempCleaner);
    }

    async start() {
        for (const c of crons) { // eslint-disable-line no-restricted-syntax 
            await c.clean(); // eslint-disable-line no-await-in-loop
        }
    }
}
module.exports = new CleanerManager();
