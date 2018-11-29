const chai = require('chai');
const moment = require('moment');
const { expect } = chai;
const fs = require('fs-extra');
const configIt = require('@hkube/config');
const sinon = require('sinon');
const config = configIt.load().main;
const cleaner = require('../lib/cleaner');
const bootstrap = require('../bootstrap');
const storageManager = require('@hkube/storage-manager');
const BUCKET_NAME = 'storage-cleaner-test';
const path = require('path');
const STORAGE_PREFIX = {
    HKUBE: 'hkube',
    HKUBE_RESULTS: 'hkube-results',
    HKUBE_METADATA: 'hkube-metadata',
    HKUBE_STORE: 'hkube-store',
    HKUBE_EXECUTION: 'hkube-execution',
    HKUBE_INDEX: 'hkube-index'
}

describe('dummy test', () => {
    before(async () => {
        config.objectExpiration = 30;
        await storageManager.init(config, true);
        await bootstrap.init();
    });
    it('clean old objects', async () => {
        await cleaner.clean();
        const jobId = Date.now();

        for (let i = 0; i < 5; i++) {
            await storageManager.put({ path: path.join('hkube-index', moment().subtract(40 + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i), data: [] });
            await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task1', data: { test: 'test1' } });
            await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task2', data: { test: 'test2' } });
            await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task3', data: { test: 'test3' } });
            await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task4', data: { test: 'test4' } });
            await storageManager.hkube.put({ jobId: 'job' + i, taskId: 'task5', data: { test: 'test5' } });
        }
        let t = await cleaner.clean();

        const result = [];
        for (let i = 0; i < 5; i++) {
            const a = await storageManager.get({ path: path.join('hkube-index', moment().subtract(40 + i, 'days').format(storageManager.hkubeIndex.DateFormat), 'job' + i) });
            const b = await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task1' });
            const c = await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task2' });
            const d = await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task3' });
            const e = await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task4' });
            const f = await storageManager.hkube.get({ jobId: 'job' + i, taskId: 'task5' });
            expect(a).to.have.property('error');
            expect(b).to.have.property('error');
            expect(c).to.have.property('error');
            expect(d).to.have.property('error');
            expect(e).to.have.property('error');
            expect(f).to.have.property('error');
        }
    }).timeout(5000);
    it('skip if not expired', async () => {
        await cleaner.clean();
        const jobId = Date.now();

        for (let i = 0; i < 5; i++) {
            await storageManager.put({ path: path.join('hkube-index', moment().format(storageManager.hkubeIndex.DateFormat), 'jobx' + i), data: [] });
            await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task1', data: { test: 'test1' } });
            await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task2', data: { test: 'test2' } });
            await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task3', data: { test: 'test3' } });
            await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task4', data: { test: 'test4' } });
            await storageManager.hkube.put({ jobId: 'jobx' + i, taskId: 'task5', data: { test: 'test5' } });
        }
        let t = await cleaner.clean();

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
        }
    }).timeout(5000);
    after(() => {
        Object.values(STORAGE_PREFIX).forEach(dir => fs.removeSync(dir));
    });
});