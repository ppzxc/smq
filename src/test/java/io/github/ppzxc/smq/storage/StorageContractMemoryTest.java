package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class StorageContractMemoryTest extends StorageContractTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.MEMORY;
    }

    // 변경 힌트는 ADR-0002가 허용한 선택 메서드라 계약이 아니다. 메모리 저장소 고유의 테스트로 둔다.

    @Test
    void 변경_힌트는_wake하면_기다리던_스레드를_즉시_깨운다() throws Exception {
        ChangeHint hint = s.changeHint().orElseThrow();
        long seen = hint.version();
        var woke = new CountDownLatch(1);
        Thread.ofPlatform().start(() -> {
            try {
                hint.await(seen, Duration.ofSeconds(30));
                woke.countDown();
            } catch (InterruptedException ignored) {
            }
        });
        Thread.sleep(100);
        hint.wake();
        assertTrue(woke.await(2, TimeUnit.SECONDS));
    }

    @Test
    void 힌트는_이미_바뀐_버전을_보고_기다리면_바로_돌아온다() throws Exception {
        ChangeHint hint = s.changeHint().orElseThrow();
        long seen = hint.version();
        hint.wake();
        long start = System.nanoTime();
        hint.await(seen, Duration.ofSeconds(30));
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 1000);
    }
}
