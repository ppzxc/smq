package io.github.ppzxc.smq;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 다중 프로세스 테스트가 별도 JVM으로 띄우는 작업자. 공개 API만 쓴다.
 * 인자: DB 경로, 작업자 이름, 발행 개수, 모드({@code pull}, {@code pull-wait} 또는 {@code publish-only}).
 * 발행을 마치면 {@code PUBLISHED}를, 꺼내 정산한 메시지마다 {@code GOT <payload>}를 표준 출력에 한 줄씩 쓴다.
 * {@code pull}이면 표준 입력이 닫힐 때까지 짧은 대기로 꺼낸다. {@code pull-wait}는 긴 대기 하나로 폴링 간격이
 * 상한까지 늘어난 상태를 만든다(프로세스를 죽여서 끝낸다). 발행 뒤 꺼내기 직전에 {@code READY}를 쓴다.
 */
public final class MultiProcessWorker {

    private MultiProcessWorker() {}

    public static void main(String[] args) throws Exception {
        Path db = Path.of(args[0]);
        String name = args[1];
        int count = Integer.parseInt(args[2]);
        boolean pull = args[3].startsWith("pull");
        Duration wait = args[3].equals("pull-wait") ? Duration.ofSeconds(60) : Duration.ofMillis(100);
        AtomicBoolean stop = new AtomicBoolean();
        Thread stdin = new Thread(() -> {
            try {
                while (System.in.read() != -1) {
                    // 입력이 닫힐 때까지 읽는다.
                }
            } catch (java.io.IOException ignored) {
                // 닫힌 것으로 본다.
            }
            stop.set(true);
        });
        stdin.setDaemon(true);
        stdin.start();
        try (Smq smq = Smq.sqlite(db).leaseDuration(Duration.ofSeconds(120)).open()) {
            Queue q = smq.declare("q", QueueOptions.defaults());
            for (int i = 0; i < count; i++) {
                q.publish(OutgoingMessage.of((name + "-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            System.out.println("PUBLISHED");
            System.out.flush();
            System.out.println("READY");
            System.out.flush();
            while (pull && !stop.get()) {
                Chunk chunk = q.pull(5, wait);
                if (chunk.isEmpty()) {
                    continue;
                }
                if (!q.settle(chunk.ackAll()).allOk()) {
                    System.out.println("SETTLE_FAILED");
                }
                for (Message m : chunk.messages()) {
                    System.out.println("GOT " + new String(m.payload(), StandardCharsets.UTF_8));
                }
                System.out.flush();
            }
        }
    }
}
