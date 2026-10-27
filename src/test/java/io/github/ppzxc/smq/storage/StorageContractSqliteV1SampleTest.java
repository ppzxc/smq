package io.github.ppzxc.smq.storage;

class StorageContractSqliteV1SampleTest extends StorageContractTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.SQLITE_V1_SAMPLE;
    }
}
