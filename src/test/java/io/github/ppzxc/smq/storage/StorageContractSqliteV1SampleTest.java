package io.github.ppzxc.smq.storage;

import org.junit.jupiter.api.Order;

@Order(1)
class StorageContractSqliteV1SampleTest extends StorageContractTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.SQLITE_V1_SAMPLE;
    }
}
