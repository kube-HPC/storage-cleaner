const configIt = require('@hkube/config');
const Logger = require('@hkube/logger');
const { main, logger } = configIt.load();
const log = new Logger(main.serviceName, logger);
const componentName = require('./common/consts/componentNames');
const { tracer, metrics } = require('@hkube/metrics');
const cleaner = require('./lib/cleaner');
const modules = [
    './lib/datastore/datastore-factory.js',
    './lib/cleaner.js'
];

class Bootstrap {
    async init() { // eslint-disable-line
        try {
            this._handleErrors();
            log.info('running application in ' + configIt.env() + ' environment', { component: componentName.MAIN });
            await metrics.init(main.metrics);
            await tracer.init(main.tracer);
            for (const m of modules) {// eslint-disable-line
                await require(m).init(main, log);// eslint-disable-line
            }
            await cleaner.cleanUpExpiredObjects();
            return main;
        }
        catch (error) {
            this._onInitFailed(error);
        }
    }

    _onInitFailed(error) {
        if (log) {
            log.error(error.message, { component: componentName.MAIN }, error);
            log.error(error);
        }
        else {
            console.error(error.message); // eslint-disable-line
            console.error(error); // eslint-disable-line
        }
        process.exit(1);
    }

    _handleErrors() {
        process.on('exit', (code) => {
            log.info('exit' + (code ? ' code ' + code : ''), { component: componentName.MAIN });
        });
        process.on('SIGINT', () => {
            log.info('SIGINT', { component: componentName.MAIN });

            process.exit(1);
        });
        process.on('SIGTERM', () => {
            log.info('SIGTERM', { component: componentName.MAIN });
            process.exit(1);
        });
        process.on('unhandledRejection', (error) => {
            log.error('unhandledRejection: ' + error.message, { component: componentName.MAIN }, error);
            log.error(error);
        });
        process.on('uncaughtException', (error) => {
            log.error('uncaughtException: ' + error.message, { component: componentName.MAIN }, error);
            log.error(JSON.stringify(error));
            process.exit(1);
        });
    }
}

module.exports = new Bootstrap();

