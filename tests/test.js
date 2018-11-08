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
const path = require('path');

describe('dummy test', () => {
    before(async () => {
        config.objectExpiration = 30;
        await DatastoreFactory.init(config, null, true);
        await bootstrap.init();
    });
    it('clean old objects s3', async () => {
        if (config.defaultStorage != 's3') return;
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
    }).timeout(5000);
    it('clean old objects fs', async () => {
        if (config.defaultStorage != 'fs') return;
        const dir = config.fs.baseDirectory;
        let adapter = DatastoreFactory.getAdapter();
        await cleaner.clean();
        const jobId = Date.now().toString();

        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(40, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test1.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(41, 'days').format(adapter.DateFormat)}`, 'jobid4'), FileName: 'test2.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(42, 'days').format(adapter.DateFormat)}`, 'jobid1'), FileName: 'test3.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(43, 'days').format(adapter.DateFormat)}`, 'jobid2'), FileName: 'test4.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(44, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test5.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(45, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test6.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(46, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test7.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(47, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test8.json', Data: { data: 'sss' } });

        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(1, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test1.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(2, 'days').format(adapter.DateFormat)}`, 'jobid4'), FileName: 'test2.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(3, 'days').format(adapter.DateFormat)}`, 'jobid1'), FileName: 'test3.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(4, 'days').format(adapter.DateFormat)}`, 'jobid2'), FileName: 'test4.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(5, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test5.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(6, 'days').format(adaptller.DateFormat)}`, 'jobid3'), FileName: 'test6.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(7, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test7.json', Data: { data: 'sss' } });
        await adapter._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(8, 'days').format(adapter.DateFormat)}`, 'jobid3'), FileName: 'test8.json', Data: { data: 'sss' } });

        const results = await Promise.all([
            adapter.put({ jobId, taskId: '0', data: 'test0' }),
            adapter.put({ jobId, taskId: '1', data: 'test1' }),
            adapter.put({ jobId, taskId: '2', data: 'test2' }),
            adapter.put({ jobId, taskId: '3', data: 'test3' }),
            adapter.put({ jobId, taskId: '4', data: 'test4' }),
            adapter.put({ jobId, taskId: '5', data: 'test5' }),
            adapter.put({ jobId, taskId: '6', data: 'test6' })]);

        let t = await cleaner.clean();
    });
});