const moment = require('moment');

class BaseCleaner {
    init({ objectExpiration, cronPattern, name }, log) {
        this.objExpiration = objectExpiration;
        this.cronPattern = cronPattern;
        this.name = name;
        this.log = log;
    }

    isExpired(date) {
        if (moment(date).isBefore(moment().subtract(this.objExpiration, 'days'))) {
            return true;
        }
        return false;
    }
}

module.exports = BaseCleaner;
