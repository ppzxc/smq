package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@ParameterizedClass
@EnumSource(Durability.class)
class SqliteSmqTest extends SmqConformanceTest {

    @Parameter
    Durability durability;

    @Override
    StorageFactory factory() {
        return StorageFactory.sqliteNew(durability);
    }

    private final List<Smq> direct = new ArrayList<>();

    @AfterEach
    void closeDirect() {
        direct.forEach(Smq::close);
    }

    private Smq.SqliteBuilder builder() {
        return Smq.sqlite(dir.resolve("smq.db"));
    }

    private Smq openDirect(Duration lease) {
        Smq smq = builder().clock(clock).leaseDuration(lease).open();
        direct.add(smq);
        return smq;
    }

    @Test
    void 다시_열어도_선언한_옵션과_참조가_남아_있고_시작할_때마다_안심하고_선언할_수_있다() {
        QueueOptions options = QueueOptions.defaults().maxDeliveries(3, "dead").singleActiveConsumer(true);
        try (Smq first = builder().open()) {
            first.declare("dead", QueueOptions.defaults());
            first.declare("orders", options);
        }
        try (Smq second = builder().open()) {
            second.declare("dead", QueueOptions.defaults());
            assertEquals(options, second.declare("orders", options).options());
            assertEquals(options, second.queue("orders").options());
            assertThrows(
                    QueueDeclarationConflictException.class, () -> second.declare("orders", QueueOptions.defaults()));
            assertThrows(QueueInUseException.class, () -> second.delete("dead"));
        }
    }

    @Test
    void 닫아도_정산하지_않은_리스는_남고_다시_연_뒤_만료되면_재전달된다() throws Exception {
        MessageId id;
        Chunk lost;
        try (Smq first = builder().clock(clock).open()) {
            Queue q = first.declare("q", QueueOptions.defaults());
            id = q.publish(msg("a"));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10));
        }
        try (Smq second = builder().clock(clock).open()) {
            Queue q = second.queue("q");
            assertEquals(new QueueCounts(0, 1, 0), q.counts());
            assertTrue(q.pull(1, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(10));
            // 기한이 지나도 다시 꺼내기 전까지는 리스 중으로 센다. 저장된 상태를 따른다.
            assertEquals(new QueueCounts(0, 1, 0), q.counts());
            Chunk again = q.pull(1, Duration.ZERO);
            assertEquals(id, again.messages().getFirst().id());
            assertEquals(2, again.messages().getFirst().deliveryCount());
            assertEquals(Outcome.STALE, q.settle(lost.ackAll()).byMessage().get(id));
            assertTrue(q.settle(again.ackAll()).allOk());
        }
    }

    @Test
    void 닫기_전에_꺼낸_리스는_다시_연_뒤_기한_전이면_그대로_정산된다() throws Exception {
        Chunk held;
        try (Smq first = builder().clock(clock).open()) {
            Queue q = first.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            held = q.pull(1, Duration.ZERO);
        }
        try (Smq second = builder().clock(clock).open()) {
            assertTrue(second.queue("q").settle(held.ackAll()).allOk());
        }
    }

    @Test
    void 호출_순서와_상관없이_durability와_공용_설정을_체인할_수_있다() {
        try (Smq a = Smq.sqlite(dir.resolve("a.db"))
                        .clock(clock)
                        .durability(Durability.PROCESS_CRASH)
                        .leaseDuration(Duration.ofSeconds(1))
                        .open();
                Smq b = Smq.sqlite(dir.resolve("b.db"))
                        .durability(Durability.PROCESS_CRASH)
                        .clock(clock)
                        .leaseDuration(Duration.ofSeconds(1))
                        .open()) {
            assertNotNull(a.declare("q", QueueOptions.defaults()));
            assertNotNull(b.declare("q", QueueOptions.defaults()));
        }
    }

    @Test
    void 기본_내구성_수준은_PROCESS_CRASH다() {
        // ADR-0014: 기본은 애플리케이션 크래시까지 견디고, 전원·OS 장애까지 견디려면 POWER_LOSS를 명시한다.
        assertEquals(
                Durability.PROCESS_CRASH, Smq.sqlite(dir.resolve("default.db")).durability());
    }

    @Test
    void SQLite_빌더의_공개_옵션에_lock_대기_하한이_없다() {
        // 0.1.0 호환 파괴(#155): 하한은 내부 상수(기본 100ms)이고, 테스트만 같은 패키지에서 조정한다.
        // 저장소 구현(storage.sqlite)은 module-info가 내보내지 않으므로 빌더가 유일한 공개 경로다.
        // 이름이 바뀐 재노출도 잡도록 공개 메서드 전체를 허용 목록과 견준다.
        Set<String> publicMethods = Arrays.stream(Smq.SqliteBuilder.class.getMethods())
                .filter(method -> method.getDeclaringClass() == Smq.SqliteBuilder.class)
                .map(Method::getName)
                .collect(Collectors.toSet());
        assertEquals(Set.of("durability", "clock", "leaseDuration", "open"), publicMethods);
        assertEquals(0, Smq.SqliteBuilder.class.getFields().length);
    }

    @Test
    void 두_내구성_수준_모두_발행과_꺼내기가_동작한다() throws Exception {
        for (Durability d : Durability.values()) {
            try (Smq smq = Smq.sqlite(dir.resolve(d + ".db")).durability(d).open()) {
                Queue q = smq.declare("q", QueueOptions.defaults());
                q.publish(msg("a"));
                assertEquals(1, q.pull(1, Duration.ZERO).messages().size());
            }
        }
    }

    @Test
    void lock_대기_하한을_없애_빈_청크가_실제로_나오는_조건에서도_동시_꺼내기_계약을_지킨다() throws Exception {
        Smq smq = builder()
                .clock(clock)
                .leaseDuration(Duration.ofMinutes(10))
                .lockWaitFloor(Duration.ZERO)
                .open();
        direct.add(smq);

        // 빈 청크는 스레드 타이밍에 달려 한 판(300라운드)에 안 나올 수 있다. 계약 검사는 매 판 모두 하고, 빈 청크는 상한까지 판을 거듭해 본다.
        int attempts = 10;
        int roundsWithEmptyChunk = 0;
        for (int attempt = 0; attempt < attempts && roundsWithEmptyChunk == 0; attempt++) {
            roundsWithEmptyChunk = assertConcurrentPullContract(smq, "race" + attempt + "-");
        }

        assertTrue(roundsWithEmptyChunk > 0, "하한을 없앴는데도 " + attempts + "판 동안 빈 청크가 한 번도 나오지 않아 이 조건이 경합을 만들지 못했다");
    }
}
