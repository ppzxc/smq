package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class SqliteSmqTest extends SmqConformanceTest {

    @Override
    Smq.Builder builder(Path dir) {
        return Smq.sqlite(dir.resolve("smq.db"));
    }

    @Test
    void 다시_열어도_선언한_옵션과_참조가_남아_있고_시작할_때마다_안심하고_선언할_수_있다() {
        QueueOptions options = QueueOptions.defaults().maxDeliveries(3, "dead").singleActiveConsumer(true);
        try (Smq first = builder(dir).open()) {
            first.declare("dead", QueueOptions.defaults());
            first.declare("orders", options);
        }
        try (Smq second = builder(dir).open()) {
            second.declare("dead", QueueOptions.defaults());
            assertEquals(options, second.declare("orders", options).options());
            assertEquals(options, second.queue("orders").options());
            assertThrows(QueueDeclarationConflictException.class,
                    () -> second.declare("orders", QueueOptions.defaults()));
            assertThrows(QueueInUseException.class, () -> second.delete("dead"));
        }
    }
}
