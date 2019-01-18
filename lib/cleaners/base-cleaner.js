const moment = require('moment');

class BaseCleaner {
    init({ objectExpiration, cronPattern, name }) {
        this.objExpiration = objectExpiration;
        this.cronPattern = cronPattern;
        this.name = name;
    }

    isExpired(date) {
        if (moment(date).isBefore(moment().subtract(this.objExpiration, 'days'))) {
            return true;
        }
        return false;
    }
}

module.exports = BaseCleaner;
