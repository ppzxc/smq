package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.StorageFactory;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

/** 견본 복사본 위에서 적합성 시나리오 전체를 돌린다. 견본의 큐 이름과 겹치지 않으므로 시나리오는 그대로 통과해야 한다. */
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
