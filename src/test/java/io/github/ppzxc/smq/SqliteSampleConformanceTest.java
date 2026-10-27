package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.StorageFactory;

/** 견본 복사본 위에서 적합성 시나리오 전체를 돌린다. 견본의 큐 이름과 겹치지 않으므로 시나리오는 그대로 통과해야 한다. */
class SqliteSampleConformanceTest extends SmqConformanceTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.SQLITE_V1_SAMPLE;
    }
}
