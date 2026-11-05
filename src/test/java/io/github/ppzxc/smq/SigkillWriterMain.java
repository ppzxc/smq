package io.github.ppzxc.smq;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/** SIGKILL 테스트가 별도 JVM으로 띄우는 프로그램. {@code PROCESS_CRASH}로 열고, 발행이 돌아올 때마다 id를 한 줄씩 쓴다. 스스로 끝나지 않는다. */
public final class SigkillWriterMain {

    private SigkillWriterMain() {}

    public static void main(String[] args) {
        Smq smq = Smq.sqlite(Path.of(args[0]))
                .durability(Durability.PROCESS_CRASH)
                .open();
        Queue q = smq.declare("q", QueueOptions.defaults());
        System.out.println("READY");
        System.out.flush();
        // 부모가 죽일 때까지 도는 것이 이 프로그램의 목적이다.
        for (long i = 0; ; i++) {
            MessageId id = q.publish(OutgoingMessage.of(("m" + i).getBytes(StandardCharsets.UTF_8)));
            System.out.println(id.value());
            System.out.flush();
        }
    }
}
