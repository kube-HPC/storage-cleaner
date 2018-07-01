const chai = require('chai');
const moment = require('moment');
const { expect } = chai;
const configIt = require('@hkube/config');
const sinon = require('sinon');
const config = configIt.load().main;
const cleaner = require('../lib/cleaner');
const bootstrap = require('../bootstrap');
const DatastoreFactory = require('../lib/datastore/datastore-factory');
const BUCKET_NAME = 'storage-cleaner-test';

describe('dummy test', () => {
    before(async () => {
        config.ObjectExpiration = 0;
        await DatastoreFactory.init(config, null, true);
        await bootstrap.init();
    });
    it('clean old objects', async () => {
        let adapter = DatastoreFactory.getAdapter();
        await cleaner.cleanUpExpiredObjects();
        const jobId = Date.now();
        const results = await Promise.all([
            adapter.put({ jobId, taskId: '0', data: 'test0' }),
            adapter.put({ jobId, taskId: '1', data: 'test1' }),
            adapter.put({ jobId, taskId: '2', data: 'test2' }),
            adapter.put({ jobId, taskId: '3', data: 'test3' }),
            adapter.put({ jobId, taskId: '4', data: 'test4' }),
            adapter.put({ jobId, taskId: '5', data: 'test5' }),
            adapter.put({ jobId, taskId: '6', data: 'test6' })]);

        let t = await cleaner.cleanUpExpiredObjects();
        expect(t.length).to.equal(7);
    });
});