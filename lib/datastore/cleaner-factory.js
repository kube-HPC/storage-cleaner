const DatastoreFactory = require('./datastore-factory');

class CleanerFactory {
    constructor(config) {
        const adapter = DatastoreFactory.getAdapter();
        if (config.deleteDestination === 'results') {
            return {
                delete: async (options) => {
                    return adapter.deleteResultsByDate(options);
                },
                list: async () => adapter.listObjectsResults(),
                DateFormat: () => adapter.DateFormat
            };
        }
        return {
            delete: async (options) => {
                return adapter.deleteByDate(options);
            },
            list: async () => adapter.listObjects(),
            DateFormat: () => adapter.DateFormat
        };
    }
}
module.exports = CleanerFactory;
