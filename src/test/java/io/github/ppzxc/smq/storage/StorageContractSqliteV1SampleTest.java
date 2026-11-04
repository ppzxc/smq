package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.Durability;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@ParameterizedClass
@EnumSource(Durability.class)
@Order(1)
class StorageContractSqliteV1SampleTest extends StorageContractTest {

    @Parameter
    Durability durability;

    @Override
    StorageFactory factory() {
        return StorageFactory.sqliteV1Sample(durability);
    }
}
