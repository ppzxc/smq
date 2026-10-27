package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.Json;
import io.github.ppzxc.smq.benchmark.random.Delivery.Action;
import io.github.ppzxc.smq.benchmark.random.Delivery.Pull;
import io.github.ppzxc.smq.benchmark.random.Delivery.Settle;
import java.util.LinkedHashMap;
import java.util.Map;

/** 기록을 JSON 한 줄로 쓴다. 실패한 실행의 발행·전달 기록 파일이 쓴다. */
final class RecordsJson {

    private RecordsJson() {}

    static String line(Published p) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tag", p.tag().toString());
        m.put("checksum", p.content().payloadChecksum());
        m.put("headers", p.content().headers());
        m.put("atMillis", p.atMillis());
        m.put("delayMillis", p.delayMillis());
        return Json.write(m);
    }

    static String line(Delivery d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("queue", d.queue().name());
        m.put("tag", d.tag().toString());
        m.put("messageId", d.messageId());
        m.put("deliveryCount", d.deliveryCount());
        m.put("checksum", d.content().payloadChecksum());
        m.put("headers", d.content().headers());
        m.put("chunk", d.pull().chunk());
        m.put("position", d.pull().position());
        m.put("pulledAtMillis", d.pull().atMillis());
        m.put("leaseDeadlineMillis", d.pull().leaseDeadlineMillis());
        m.put("settle", d.settle().action().name());
        m.put("applied", d.settle().applied());
        m.put("settleAtMillis", d.settle().atMillis());
        m.put("nackDelayMillis", d.settle().nackDelayMillis());
        return Json.write(m);
    }

    static Published parsePublished(String line) {
        Map<?, ?> m = (Map<?, ?>) Json.parse(line);
        return new Published(Tag.parse((String) m.get("tag")), content(m), num(m, "atMillis"),
                num(m, "delayMillis"));
    }

    static Delivery parseDelivery(String line) {
        Map<?, ?> m = (Map<?, ?>) Json.parse(line);
        Pull pull = new Pull(num(m, "chunk"), (int) num(m, "position"), num(m, "pulledAtMillis"),
                num(m, "leaseDeadlineMillis"));
        Settle settle = new Settle(Action.valueOf((String) m.get("settle")), (Boolean) m.get("applied"),
                num(m, "settleAtMillis"), num(m, "nackDelayMillis"));
        return new Delivery(QueueKind.valueOf((String) m.get("queue")), Tag.parse((String) m.get("tag")),
                num(m, "messageId"), (int) num(m, "deliveryCount"), content(m), pull, settle);
    }

    private static Content content(Map<?, ?> m) {
        Map<String, String> headers = new LinkedHashMap<>();
        ((Map<?, ?>) m.get("headers")).forEach((k, v) -> headers.put((String) k, (String) v));
        return new Content(num(m, "checksum"), headers);
    }

    private static long num(Map<?, ?> m, String key) {
        return ((Double) m.get(key)).longValue();
    }

    static String line(CountsSample s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("queue", s.queue().name());
        m.put("afterDrain", s.afterDrain());
        m.put("ready", s.ready());
        m.put("leased", s.leased());
        m.put("delayed", s.delayed());
        return Json.write(m);
    }

    static String describe(Violation v) {
        return v.invariant() + ": " + v.detail() + " [tags=" + v.tags() + ", messageIds=" + v.messageIds() + "]";
    }
}
