const CleanerFactory = require('../lib/datastore/cleaner-factory');
const component = require('../common/consts/componentNames').MAIN;
const moment = require('moment');
const groupBy = require('lodash.groupby');

class Cleaner {
    init(config, log) {
        this._log = log;
        this._objectExpiration = config.objectExpiration;
        this._adapter = new CleanerFactory(config);
    }

    async clean() {
        const res = await this._adapter.list();
        const dates = groupBy(res, str => str.Path.split('/')[1]);
        const deletePromises = [];

        Object.entries(dates).forEach((file) => {
            if (this._isValid(file[0]) && this._isExpired(file[0])) {
                file[1].forEach(f => deletePromises.push(this._adapter.delete(f)));
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

    _isValid(date) {
        if (!moment(date, this._adapter.DateFormat(), true).isValid()) {
            this._log.error(`failed to clean storage, invalid date: ${date}`, { component });
            return false;
        }
        return true;
    }
}

module.exports = new Cleaner();
