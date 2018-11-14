const storageManager = require('@hkube/storage-manager');

class CleanerFactory {
    constructor(config) {
        if (config.deleteDestination === 'results') {
            return {
                delete: async (options) => {
                    return storageManager.deleteResults(options);
                },
                list: async () => storageManager.listResults(),
                DateFormat: () => storageManager.DateFormat
            };
        }
        return {
            delete: async (options) => {
                return storageManager.delete(options);
            },
            list: async () => storageManager.list(),
            DateFormat: () => storageManager.DateFormat
        };
    }
}
module.exports = CleanerFactory;
