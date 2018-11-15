const chai = require('chai');
const moment = require('moment');
const { expect } = chai;
const configIt = require('@hkube/config');
const sinon = require('sinon');
const config = configIt.load().main;
const cleaner = require('../lib/cleaner');
const bootstrap = require('../bootstrap');
const storageManager = require('@hkube/storage-manager');
const BUCKET_NAME = 'storage-cleaner-test';
const path = require('path');

describe('dummy test', () => {
    before(async () => {
        config.objectExpiration = 30;
        await storageManager.init(config, true);
        await bootstrap.init();
    });
    it('clean old objects s3', async () => {
        if (config.defaultStorage != 's3') return;
        await cleaner.clean();
        const jobId = Date.now();

        await storageManager._put({ Path: path.join('hkube', moment().subtract(40, 'days').format(storageManager.DateFormat), 'test3', 'test3.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(41, 'days').format(storageManager.DateFormat), 'test4', 'test4.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(42, 'days').format(storageManager.DateFormat), 'test1', 'test2.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(43, 'days').format(storageManager.DateFormat), 'test2', 'test3.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(44, 'days').format(storageManager.DateFormat), 'test3', 'test4.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(45, 'days').format(storageManager.DateFormat), 'test3', 'test5.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(46, 'days').format(storageManager.DateFormat), 'test3', 'test6.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(47, 'days').format(storageManager.DateFormat), 'test3', 'test7.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(47, 'days').format(storageManager.DateFormat), 'test44', 'test7.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(47, 'days').format(storageManager.DateFormat), 'test55', 'test7.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(47, 'days').format(storageManager.DateFormat), 'test66', 'test7.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(47, 'days').format(storageManager.DateFormat), 'test77', 'test7.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(47, 'days').format(storageManager.DateFormat), 'test88', 'test7.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(47, 'days').format(storageManager.DateFormat), 'test99', 'test7.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(47, 'days').format(storageManager.DateFormat), 'test00', 'test7.json'), Data: { data: 'sss' } });

        await storageManager._put({ Path: path.join('hkube', moment().subtract(2, 'days').format(storageManager.DateFormat), 'test4', 'test3.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(3, 'days').format(storageManager.DateFormat), 'test1', 'test4.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(4, 'days').format(storageManager.DateFormat), 'test2', 'test2.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(5, 'days').format(storageManager.DateFormat), 'test3', 'test3.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(6, 'days').format(storageManager.DateFormat), 'test3', 'test4.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(7, 'days').format(storageManager.DateFormat), 'test3', 'test5.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(8, 'days').format(storageManager.DateFormat), 'test3', 'test6.json'), Data: { data: 'sss' } });
        await storageManager._put({ Path: path.join('hkube', moment().subtract(1, 'days').format(storageManager.DateFormat), 'test3', 'test7.json'), Data: { data: 'sss' } });

        const results = await Promise.all([
            storageManager.put({ jobId: jobId.toString(), taskId: '0', data: 'test0' }),
            storageManager.put({ jobId: jobId.toString(), taskId: '1', data: 'test1' }),
            storageManager.put({ jobId: jobId.toString(), taskId: '2', data: 'test2' }),
            storageManager.put({ jobId: jobId.toString(), taskId: '3', data: 'test3' }),
            storageManager.put({ jobId: jobId.toString(), taskId: '4', data: 'test4' }),
            storageManager.put({ jobId: jobId.toString(), taskId: '5', data: 'test5' }),
            storageManager.put({ jobId: jobId.toString(), taskId: '6', data: 'test6' })]);

        let t = await cleaner.clean();
        let countDeletedObjects = 0;
        t.forEach(x => countDeletedObjects += x.Deleted.length)
        expect(countDeletedObjects).to.equal(15);
    }).timeout(5000);
    it('clean old objects fs', async () => {
        if (config.defaultStorage != 'fs') return;
        const dir = config.fs.baseDirectory;
        await cleaner.clean();
        const jobId = Date.now().toString();

        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(40, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test1.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(41, 'days').format(storageManager.DateFormat)}`, 'jobid4'), FileName: 'test2.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(42, 'days').format(storageManager.DateFormat)}`, 'jobid1'), FileName: 'test3.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(43, 'days').format(storageManager.DateFormat)}`, 'jobid2'), FileName: 'test4.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(44, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test5.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(45, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test6.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(46, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test7.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(47, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test8.json', Data: { data: 'sss' } });

        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(1, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test1.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(2, 'days').format(storageManager.DateFormat)}`, 'jobid4'), FileName: 'test2.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(3, 'days').format(storageManager.DateFormat)}`, 'jobid1'), FileName: 'test3.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(4, 'days').format(storageManager.DateFormat)}`, 'jobid2'), FileName: 'test4.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(5, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test5.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(6, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test6.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(7, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test7.json', Data: { data: 'sss' } });
        await storageManager._put({ DirectoryName: path.join(dir, 'hkube', `${moment().subtract(8, 'days').format(storageManager.DateFormat)}`, 'jobid3'), FileName: 'test8.json', Data: { data: 'sss' } });

        const results = await Promise.all([
            storageManager.put({ jobId, taskId: '0', data: 'test0' }),
            storageManager.put({ jobId, taskId: '1', data: 'test1' }),
            storageManager.put({ jobId, taskId: '2', data: 'test2' }),
            storageManager.put({ jobId, taskId: '3', data: 'test3' }),
            storageManager.put({ jobId, taskId: '4', data: 'test4' }),
            storageManager.put({ jobId, taskId: '5', data: 'test5' }),
            storageManager.put({ jobId, taskId: '6', data: 'test6' })]);

        let t = await cleaner.clean();
    });
});