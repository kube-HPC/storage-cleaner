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
        config.objectExpiration = 30;
        await DatastoreFactory.init(config, null, true);
        await bootstrap.init();
    });
    it('clean old objects', async () => {
        let adapter = DatastoreFactory.getAdapter();
        await cleaner.clean();
        const jobId = Date.now();

        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(40, 'days').format(adapter.DateFormat)}/test3/test3.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(41, 'days').format(adapter.DateFormat)}/test4/test4.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(42, 'days').format(adapter.DateFormat)}/test1/test2.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(43, 'days').format(adapter.DateFormat)}/test2/test3.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(44, 'days').format(adapter.DateFormat)}/test3/test4.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(45, 'days').format(adapter.DateFormat)}/test3/test5.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(46, 'days').format(adapter.DateFormat)}/test3/test6.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(47, 'days').format(adapter.DateFormat)}/test3/test7.json`, Body: { data: 'sss' } });

        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(1, 'days').format(adapter.DateFormat)}/test3/test3.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(2, 'days').format(adapter.DateFormat)}/test4/test4.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(3, 'days').format(adapter.DateFormat)}/test1/test2.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(4, 'days').format(adapter.DateFormat)}/test2/test3.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(5, 'days').format(adapter.DateFormat)}/test3/test4.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(6, 'days').format(adapter.DateFormat)}/test3/test5.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(7, 'days').format(adapter.DateFormat)}/test3/test6.json`, Body: { data: 'sss' } });
        await adapter._put({ Bucket: 'hkube', Key: `${moment().subtract(8, 'days').format(adapter.DateFormat)}/test3/test7.json`, Body: { data: 'sss' } });


        const results = await Promise.all([
            adapter.put({ jobId, taskId: '0', data: 'test0' }),
            adapter.put({ jobId, taskId: '1', data: 'test1' }),
            adapter.put({ jobId, taskId: '2', data: 'test2' }),
            adapter.put({ jobId, taskId: '3', data: 'test3' }),
            adapter.put({ jobId, taskId: '4', data: 'test4' }),
            adapter.put({ jobId, taskId: '5', data: 'test5' }),
            adapter.put({ jobId, taskId: '6', data: 'test6' })]);

        let t = await cleaner.clean();
        let countDeletedObjects = 0;
        t.forEach(x => countDeletedObjects += x.Deleted.length)
        expect(countDeletedObjects).to.equal(16);
    });

});