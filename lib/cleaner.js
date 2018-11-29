const storageManager = require('@hkube/storage-manager');
const component = require('../common/consts/componentNames').MAIN;
const moment = require('moment');
const groupBy = require('lodash.groupby');

class Cleaner {
    init(config, log) {
        this._log = log;
        this._objectExpiration = config.objectExpiration;
    }

    async clean() {
        const indexes = await storageManager.hkubeIndex.list({ date: '' });
        const dates = groupBy(indexes, str => str.path.split('/')[1]);
        const jobsToDelete = [];
        const indexesToDelete = [];

        Object.entries(dates).forEach((file) => {
            if (this._isValid(file[0]) && this._isExpired(file[0])) {
                file[1].forEach((f) => {
                    jobsToDelete.push(f.path.split('/')[2]);
                    indexesToDelete.push(f.path);
                });
            }
        });
        const listTasksPromies = [];
        jobsToDelete.forEach((jobId) => {
            listTasksPromies.push(storageManager.hkube.list({ jobId }));
        });
        const tasksToDelete = await Promise.all(listTasksPromies);
        const tasksToDeletePromies = [];
        tasksToDelete.forEach(job => job.forEach(t =>
            tasksToDeletePromies.push(storageManager.delete(t))));
        await Promise.all(tasksToDeletePromies);
        const indexesToDeletePromies = [];
        indexesToDelete.forEach((index) => {
            indexesToDeletePromies.push(storageManager.delete({ path: index }));
        });
        await Promise.all(indexesToDeletePromies);
    }

    _isExpired(date) {
        if (moment(date).isBefore(moment().subtract(this._objectExpiration, 'days'))) {
            return true;
        }
        return false;
    }

    _isValid(date) {
        if (!moment(date, storageManager.DateFormat, true).isValid()) {
            this._log.error(`failed to clean storage, invalid date: ${date}`, { component });
            return false;
        }
        return true;
    }
}

module.exports = new Cleaner();
