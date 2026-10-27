package io.github.ppzxc.smq.storage;

import org.junit.jupiter.api.Order;

@Order(1)
class StorageContractSqliteTest extends StorageContractTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.SQLITE_NEW;
    }
}
