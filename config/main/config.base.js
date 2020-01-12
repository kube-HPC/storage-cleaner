const config = {};
const formatter = require(process.cwd() + '/lib/utils/formatters');

config.serviceName = 'storage-cleaner';
config.defaultStorage = process.env.DEFAULT_STORAGE || 's3';

config.cleaners = {
    results: {
        name: 'result objects',
        objectExpiration: formatter.parseInt(process.env.RESULT_OBJECT_EXPIRATION_DAYS, 10)
    },
    temp: {
        name: 'temp objects',
        objectExpiration: formatter.parseInt(process.env.TEMP_OBJECT_EXPIRATION_DAYS, 5)
    }
};

config.s3 = {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    endpoint: process.env.S3_ENDPOINT_URL || 'http://127.0.0.1:9000',
    binary: formatter.parseBool(process.env.STORAGE_BINARY, false)
};

config.fs = {
    baseDirectory: process.env.BASE_FS_ADAPTER_DIRECTORY || '',
    binary: formatter.parseBool(process.env.STORAGE_BINARY, false)
};

config.clusterName = process.env.CLUSTER_NAME || 'local';

config.storageAdapters = {
    s3: {
        connection: config.s3,
        moduleName: process.env.STORAGE_MODULE || '@hkube/s3-adapter'
    },
    etcd: {
        connection: config.etcd,
        moduleName: process.env.STORAGE_MODULE || '@hkube/etcd-adapter'
    },
    redis: {
        connection: config.redis,
        moduleName: process.env.STORAGE_MODULE || '@hkube/redis-storage-adapter'
    },
    fs: {
        connection: config.fs,
        moduleName: process.env.STORAGE_MODULE || '@hkube/fs-adapter'
    }
};

module.exports = config;
