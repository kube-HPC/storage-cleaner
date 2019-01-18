const chai = require('chai');
const moment = require('moment');
const { expect } = chai;
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
    HKUBE: 'hkube',
    HKUBE_RESULTS: 'hkube-results',
    HKUBE_METADATA: 'hkube-metadata',
    HKUBE_STORE: 'hkube-store',
    HKUBE_EXECUTION: 'hkube-execution',
    HKUBE_INDEX: 'hkube-index'
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
                mockery.registerSubstitute('@hkube/logger', process.cwd() + '/tests/mock/log.js');
                storageManager = require('@hkube/storage-manager'); // eslint-disable-line

                main.defaultStorage = adapter;
                await storageManager.init(main, true);
                cleaner = require('../lib/cleaners/cleaner-manager');
                await cleaner.init(main);
                await cleaner.start();
            });
            describe('clean storage - using: ' + adapter, () => {
                it('clean temp objects', async () => {
                    await cleaner.start();
                    const jobId = Date.now();

                    for (let i = 0; i < 5; i++) {
                        const a = main.cleaners.results;
                        await storageManager.put({ path: path.join('hkube-index', moment().subtract(main.cleaners.temp.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i), data: [] });
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

                    const temp = [];
                    const results = [];
                    for (let i = 0; i < 5; i++) {
                        await storageManager.get({ path: path.join('hkube-index', moment().subtract(main.cleaners.temp.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i) });
                        temp.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task1' }));
                        temp.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                        temp.push(await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task2' }));
                        temp.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task2' }));
                        temp.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                        temp.push(await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task3' }));
                        temp.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task3' }));
                        temp.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                        temp.push(await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task4' }));
                        temp.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task4' }));
                        temp.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                        temp.push(await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task5' }));
                        temp.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task5' }));
                        temp.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                    }
                    temp.forEach(x => expect(x).to.have.property('error'));
                    results.forEach(x => expect(x).to.not.have.property('error'));
                }).timeout(6000);
                it('clean results+temp objects', async () => {
                    await cleaner.start();
                    const jobId = Date.now();

                    for (let i = 0; i < 5; i++) {
                        const a = main.cleaners.results;
                        await storageManager.put({ path: path.join('hkube-index', moment().subtract(main.cleaners.results.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i), data: [] });
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
                        results.push(await storageManager.get({ path: path.join('hkube-index', moment().subtract(main.cleaners.results.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i) }));
                        results.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task1' }));
                        results.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                        results.push(await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task2' }));
                        results.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task2' }));
                        results.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                        results.push(await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task3' }));
                        results.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task3' }));
                        results.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                        results.push(await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task4' }));
                        results.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task4' }));
                        results.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));

                        results.push(await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task5' }));
                        results.push(await storageManager.hkubeMetadata.get({ jobId: 'job' + i, taskId: 'task5' }));
                        results.push(await storageManager.hkubeExecutions.get({ jobId: 'job' + i }));
                        results.push(await storageManager.hkubeResults.get({ jobId: 'job' + i }));
                    }
                    results.forEach(x => expect(x).to.have.property('error'));
                }).timeout(6000);
                it('get and put object', async () => {
                    await cleaner.start();
                    const jobId = Date.now();

                    for (let i = 0; i < 5; i++) {
                        await storageManager.put({ path: path.join('hkube-index', moment().format(storageManager.hkubeIndex.DateFormat), 'jobx' + i), data: [] });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task1', data: { test: 'test1' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task2', data: { test: 'test2' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task3', data: { test: 'test3' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task4', data: { test: 'test4' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task5', data: { test: 'test5' } });
                    }
                    let t = await cleaner.start();

                    const result = [];
                    for (let i = 0; i < 5; i++) {
                        const a = await storageManager.get({ path: path.join('hkube-index', moment().format(storageManager.hkubeIndex.DateFormat), 'jobx' + i) });
                        const b = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task1' });
                        const c = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task2' });
                        const d = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task3' });
                        const e = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task4' });
                        const f = await storageManager.hkube.get({ jobId: 'jobx' + i, taskId: 'task5' });
                        expect(a).to.not.have.property('error');
                        expect(b).to.not.have.property('error');
                        expect(c).to.not.have.property('error');
                        expect(d).to.not.have.property('error');
                        expect(e).to.not.have.property('error');
                        expect(f).to.not.have.property('error');
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




