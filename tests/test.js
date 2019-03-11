const chai = require('chai');
const moment = require('moment');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const Logger = require('@hkube/logger');
const fs = require('fs-extra');
const configIt = require('@hkube/config');
const { main, logger } = configIt.load();
const log = new Logger(main.serviceName, logger);
let cleaner;
let storageManager;

const path = require('path');
const mockery = require('mockery');
const adapters = ['s3', 'fs'];

const STORAGE_PREFIX = {
    HKUBE: 'local-hkube',
    HKUBE_RESULTS: 'local-hkube-results',
    HKUBE_METADATA: 'local-hkube-metadata',
    HKUBE_STORE: 'local-hkube-store',
    HKUBE_EXECUTION: 'local-hkube-execution',
    HKUBE_INDEX: 'local-hkube-index',
    HKUBE_BUILD: 'local-hkube-builds'
}

describe('cleaner tests', () => {
    adapters.forEach((adapter) => {
        describe('clean', () => {
            before(async () => {
                mockery.enable({
                    warnOnReplace: false,
                    warnOnUnregistered: false,
                    useCleanCache: true
                });
                mockery.resetCache();
                storageManager = require('@hkube/storage-manager'); // eslint-disable-line

                main.defaultStorage = adapter;
                await storageManager.init(main, log, true);
                cleaner = require('../lib/cleaners/cleaner-manager');
                await cleaner.init(main, log);
                await cleaner.start();
            });
            describe('clean storage - using: ' + adapter, () => {
                it('clean temp objects', async () => {
                    await cleaner.start();
                    const jobId = Date.now();

                    for (let i = 0; i < 5; i++) {
                        const a = main.cleaners.results;
                        await storageManager.put({ path: path.join('local-hkube-index', moment().subtract(main.cleaners.temp.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i), data: [] });
                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task1', data: { test: 'test1' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task1', data: { test: 'test1' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'test1' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'test1' } });

                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task2', data: { test: 'test2' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task2', data: { test: 'test2' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'test2' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'test2' } });

                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task3', data: { test: 'test3' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task3', data: { test: 'test3' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'test3' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'test3' } });

                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task4', data: { test: 'test4' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task4', data: { test: 'test4' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'test4' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'test4' } });

                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task5', data: { test: 'test5' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task5', data: { test: 'test5' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'task5' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'task5' } });

                    }
                    let t = await cleaner.start();
                    for (let i = 0; i < 5; i++) {
                        await storageManager.get({ path: path.join('local-hkube-index', moment().subtract(main.cleaners.temp.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i) });
                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task1' })).to.eventually.rejectedWith(Error);

                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task1' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        await storageManager.hkubeResults.get({ jobId: 'job' + i });

                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task2' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task2' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        await storageManager.hkubeResults.get({ jobId: 'job' + i });

                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task3' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task3' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        await storageManager.hkubeResults.get({ jobId: 'job' + i });

                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task4' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task4' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        await storageManager.hkubeResults.get({ jobId: 'job' + i });

                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task5' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task5' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        await storageManager.hkubeResults.get({ jobId: 'job' + i });

                    }
                }).timeout(60000);
                it('clean results+temp objects', async () => {
                    await cleaner.start();
                    const jobId = Date.now();

                    for (let i = 0; i < 5; i++) {
                        const a = main.cleaners.results;
                        await storageManager.put({ path: path.join('local-hkube-index', moment().subtract(main.cleaners.results.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i), data: [] });
                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task1', data: { test: 'test1' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task1', data: { test: 'test1' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'test1' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'test1' } });

                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task2', data: { test: 'test2' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task2', data: { test: 'test2' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'test2' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'test2' } });

                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task3', data: { test: 'test3' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task3', data: { test: 'test3' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'test3' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'test3' } });

                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task4', data: { test: 'test4' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task4', data: { test: 'test4' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'test4' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'test4' } });

                        await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task5', data: { test: 'test5' } });
                        await storageManager.hkubeMetadata.put({ jobId: 'job' + i, taskId: 'task5', data: { test: 'test5' } });
                        await storageManager.hkubeExecutions.put({ jobId: 'job' + i, data: { test: 'task5' } });
                        await storageManager.hkubeResults.put({ jobId: 'job' + i, data: { test: 'task5' } });

                    }
                    let t = await cleaner.start();

                    const results = [];
                    for (let i = 0; i < 5; i++) {
                        expect(storageManager.get({ path: path.join('local-hkube-index', moment().subtract(main.cleaners.results.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i) })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task1' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeResults.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task2' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task2' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeResults.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task3' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task3' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeResults.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task4' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task4' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeResults.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task5' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task5' })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeExecutions.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                        expect(storageManager.hkubeResults.get({ jobId: 'job' + i })).to.eventually.rejectedWith(Error);
                    }
                }).timeout(6000);
                it('get and put object', async () => {
                    await cleaner.start();
                    const jobId = Date.now();

                    for (let i = 0; i < 5; i++) {
                        await storageManager.put({ path: path.join('local-hkube-index', moment().format(storageManager.hkubeIndex.DateFormat), 'jobx' + i), data: [] });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task1', data: { test: 'test1' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task2', data: { test: 'test2' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task3', data: { test: 'test3' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task4', data: { test: 'test4' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task5', data: { test: 'test5' } });
                    }
                    let t = await cleaner.start();

                    const result = [];
                    for (let i = 0; i < 5; i++) {
                        const a = await storageManager.get({ path: path.join('local-hkube-index', moment().format(storageManager.hkubeIndex.DateFormat), 'jobx' + i) });
                        const b = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task1' });
                        const c = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task2' });
                        const d = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task3' });
                        const e = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task4' });
                        const f = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task5' });
                        await storageManager.hkube.delete({ jobId: 'jobx' + i });
                    }
                    await storageManager.hkubeIndex.delete({ date: moment().format(storageManager.hkubeIndex.DateFormat) });
                }).timeout(6000);
            });
        });
    });
    after(() => {
        Object.values(STORAGE_PREFIX).forEach(dir => fs.removeSync(path.join(main.fs.baseDirectory, dir)));
    });
});




