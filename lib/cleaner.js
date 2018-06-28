const DatastoreFactory = require('../lib/datastore/datastore-factory');
const moment = require('moment');

class Cleaner {
    init(config, log) {
        this._objectExpiration = config.ObjectExpiration;
        this._log = log;
    }

    async cleanUpExpiredObjects() {
        const storageAdapter = DatastoreFactory.getAdapter();
        const res = await storageAdapter.listObjects();
        const dates = new Set(res.map(x => new Date(x.Key.split('/')[0])));
        const deletePromises = [];

        dates.forEach(async (date) => {
            if (this._isExpired(date)) {
                deletePromises.push(storageAdapter.deleteByDate({ Date: date }));
            }
        });
        const result = await Promise.all(deletePromises);
        this._log.info(`delete info: ${JSON.stringify(result, null, 2)}`);
    }

    _isExpired(date) {
        if (moment(date).isBefore(moment().subtract(this._objectExpiration, 'days'))) {
            return true;
        }
        return false;
    }
}

module.exports = new Cleaner();
