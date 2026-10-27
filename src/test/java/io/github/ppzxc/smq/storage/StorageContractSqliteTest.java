package io.github.ppzxc.smq.storage;

class StorageContractSqliteTest extends StorageContractTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.SQLITE_NEW;
    }
}
