const storageManager = require('@hkube/storage-manager');
const moment = require('moment');

class Cleaner {
    init(config, log) {
        this._log = log;
        this._objectExpiration = config.objectExpiration;
        this._baseDir = config.fs.baseDirectory;
    }

    async clean() {
        const indexes = await storageManager.hkubeIndex.list({ date: '' });
        const parsedJobs = indexes.map(d => d.path.match(/(\d{4}-\d{2}-\d{2})\/(.*)/));
        const datesToDelete = [];
        const jobsToDelete = [];
        const deletePromiess = [];

        parsedJobs.forEach((line) => {
            if (this._isExpired(line[1])) {
                datesToDelete.push(line[1]);
                jobsToDelete.push(line[2]);
            }
        });

        jobsToDelete.forEach((jobId) => {
            deletePromiess.push(storageManager.hkube.delete({ jobId }));
            deletePromiess.push(storageManager.hkubeMetadata.delete({ jobId }));
            deletePromiess.push(storageManager.hkubeExecutions.delete({ jobId }));
        });
        await Promise.all(deletePromiess);
        const indexesToDeletePromies = [];
        datesToDelete.forEach((date) => {
            indexesToDeletePromies.push(storageManager.hkubeIndex.delete({ date }));
        });
        await Promise.all(indexesToDeletePromies);
    }

    _isExpired(date) {
        if (moment(date).isBefore(moment().subtract(this._objectExpiration, 'days'))) {
            return true;
        }
        return false;
    }
}

module.exports = new Cleaner();
