const chai = require('chai');
const path = require('path');
const fs = require('fs-extra');
const chaiAsPromised = require('chai-as-promised');
const moment = require('moment');
const mockery = require('mockery');
const configIt = require('@hkube/config');
const Logger = require('@hkube/logger');
let storageManager, cleaner;
const { expect } = chai;
chai.use(chaiAsPromised);

const { main, logger } = configIt.load();
const log = new Logger(main.serviceName, logger);
const adapters = ['s3', 'fs'];
let STORAGE_PREFIX;

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
                main.defaultStorage = adapter;
                storageManager = require('@hkube/storage-manager'); // eslint-disable-line
                await storageManager.init(main, log, true);
                STORAGE_PREFIX = storageManager.prefixesTypes;
                cleaner = require('../lib/cleaners/cleaner-manager');

                await cleaner.init(main, log);
                await cleaner.start();
            });
            describe('clean storage - using: ' + adapter, () => {
                it('clean temp objects', async () => {
                    await cleaner.start();

                    for (let i = 0; i < 5; i++) {
                        await storageManager.put({ path: path.join('local-hkube-index', moment().subtract(main.cleaners.temp.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i), data: [] });

                        for (let j = 1; j <= 5; j++) {
                            const jobId = 'job' + i;
                            const taskId = 'task' + j;
                            const data = { test: 'test' + j };
                            await storageManager.hkube.put({ jobId, taskId, data });
                            await storageManager.hkubeMetadata.put({ jobId, taskId, data });
                            await storageManager.hkubeExecutions.put({ jobId, data });
                            await storageManager.hkubeResults.put({ jobId, data });
                        }

                    }
                    await cleaner.start();
                    for (let i = 0; i < 5; i++) {
                        await storageManager.get({ path: path.join('local-hkube-index', moment().subtract(main.cleaners.temp.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i) });

                        for (let j = 1; j <= 5; j++) {
                            const jobId = 'job' + i;
                            const taskId = 'task' + j;
                            expect(storageManager.hkube.get({ jobId, taskId })).to.eventually.rejectedWith(Error);
                            expect(storageManager.hkubeMetadata.get({ jobId, taskId })).to.eventually.rejectedWith(Error);
                            expect(storageManager.hkubeExecutions.get({ jobId })).to.eventually.rejectedWith(Error);
                            await storageManager.hkubeResults.get({ jobId });
                        }
                    }
                }).timeout(60000);
                it('clean results+temp objects', async () => {
                    await cleaner.start();

                    for (let i = 0; i < 5; i++) {
                        await storageManager.put({ path: path.join('local-hkube-index', moment().subtract(main.cleaners.results.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i), data: [] });

                        for (let j = 1; j <= 5; j++) {
                            const jobId = 'job' + i;
                            const taskId = 'task' + j;
                            const data = { test: 'test' + j };
                            await storageManager.hkube.put({ jobId, taskId, data });
                            await storageManager.hkubeMetadata.put({ jobId, taskId, data });
                            await storageManager.hkubeExecutions.put({ jobId, data });
                            await storageManager.hkubeResults.put({ jobId, data });
                        }
                    }
                    await cleaner.start();

                    for (let i = 0; i < 5; i++) {
                        expect(storageManager.get({ path: path.join('local-hkube-index', moment().subtract(main.cleaners.results.objectExpiration + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i) })).to.eventually.rejectedWith(Error);

                        for (let j = 1; j <= 5; j++) {
                            const jobId = 'job' + i;
                            const taskId = 'task' + j;
                            expect(storageManager.hkubeMetadata.get({ jobId, taskId })).to.eventually.rejectedWith(Error);
                            expect(storageManager.hkubeExecutions.get({ jobId })).to.eventually.rejectedWith(Error);
                            expect(storageManager.hkubeResults.get({ jobId })).to.eventually.rejectedWith(Error);
                            expect(storageManager.hkube.get({ jobId, taskId })).to.eventually.rejectedWith(Error);
                        }
                    }
                }).timeout(10000);
                it('get and put object', async () => {
                    await cleaner.start();

                    for (let i = 0; i < 5; i++) {
                        await storageManager.put({ path: path.join('local-hkube-index', moment().format(storageManager.hkubeIndex.DateFormat), 'jobx' + i), data: [] });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task1', data: { test: 'test1' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task2', data: { test: 'test2' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task3', data: { test: 'test3' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task4', data: { test: 'test4' } });
                        await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task5', data: { test: 'test5' } });
                    }
                    await cleaner.start();

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
        STORAGE_PREFIX.forEach(dir => fs.removeSync(path.join(main.fs.baseDirectory, dir)));
    });
});




