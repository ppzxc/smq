package smqdeploytest;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.ppzxc.smq.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 외부 소비자가 공유하는 빠른 시작 코드. README의 빠른 시작과 같은 흐름이다.
 * 기대와 다르면 비정상 종료한다.
 */
public final class Main {
    public static void main(String[] args) throws Exception {
        // sqlite-jdbc가 전이 의존성으로 해석됐는지는 SQLite 저장소가 열리는지로 확인한다.
        Path dir = Files.createTempDirectory("smq-deploy-test");
        List<String> received = new ArrayList<>();
        try (Smq smq = Smq.sqlite(dir.resolve("smq.db")).open()) {
            Queue q = smq.declare("orders", QueueOptions.defaults());
            q.publish(OutgoingMessage.of("hello".getBytes(UTF_8)));

            Chunk chunk = q.pull(100, Duration.ofSeconds(5));
            for (Message m : chunk.messages()) {
                String text = new String(m.payload(), UTF_8);
                System.out.println(text);
                received.add(text);
            }
            q.settle(chunk.ackAll());

            QueueCounts counts = q.counts();
            require(counts.equals(new QueueCounts(0, 0, 0)), "ACK 뒤 counts가 (0, 0, 0)이어야 한다: " + counts);
        }
        require(received.equals(List.of("hello")), "받은 메시지가 [hello]여야 한다: " + received);

        try (Smq smq = Smq.inMemory().open()) {
            smq.declare("memory", QueueOptions.defaults());
        }
        System.out.println("OK");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            System.err.println("FAIL: " + message);
            System.exit(1);
        }
    }
}
