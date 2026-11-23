package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.QueueOptions;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@ParameterizedClass
@EnumSource(Durability.class)
@Order(1)
class StorageContractSqliteTest extends StorageContractTest {

    @Parameter
    Durability durability;

    @Test
    void 꺼내기_쓰기_트랜잭션은_시계를_다시_읽는다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        AtomicInteger reads = new AtomicInteger();
        PullResult result = storage.pull("q", 1, LEASE, () -> reads.getAndIncrement() == 0 ? T0 : T0.plusSeconds(60));
        assertEquals(2, reads.get());
        assertEquals(T0.plusSeconds(70), result.leaseDeadline());
    }

    @Override
    StorageFactory factory() {
        return StorageFactory.sqliteNew(durability);
    }
}
