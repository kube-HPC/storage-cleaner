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
        const deletePromises = [];

        Object.keys(res).forEach((date) => {
            if (this._isExpired(date)) {
                deletePromises.push(storageAdapter.deleteByDate({ Date: date }));
            }
        });

        const result = await Promise.all(deletePromises);
        this._log.info(`delete info: ${JSON.stringify(result, null, 2)}`);
        return result;
    }

    _isExpired(date) {
        if (moment(date).isBefore(moment().subtract(this._objectExpiration, 'days'))) {
            return true;
        }
        return false;
    }
}

module.exports = new Cleaner();
