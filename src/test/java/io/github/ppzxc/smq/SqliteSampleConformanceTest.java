package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.StorageFactory;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@ParameterizedClass
@EnumSource(Durability.class)
class SqliteSampleConformanceTest extends SmqConformanceTest {

    @Parameter
    Durability durability;

    @Override
    StorageFactory factory() {
        return StorageFactory.sqliteV1Sample(durability);
    }
}
