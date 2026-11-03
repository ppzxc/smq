package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.random.Delivery.Action;
import io.github.ppzxc.smq.benchmark.random.Delivery.Pull;
import io.github.ppzxc.smq.benchmark.random.Delivery.Settle;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 발행과 전달 기록의 이진 형식. 긴 실행의 기록을 디스크에 쌓았다가 읽으려고 쓴다. JSON은 숫자를 {@code double}로 읽어 64비트
 * 체크섬의 아래 비트를 잃고, 수천만 건에는 너무 크고 느리다. 한 기록의 끝이 잘려 있으면(쓰던 프로세스가 죽었다) 그 기록은 없는 것으로
 * 보고 파일 끝으로 친다.
 */
final class RecordCodec {

    private static final Action[] ACTIONS = Action.values();
    private static final QueueKind[] QUEUES = QueueKind.values();

    private RecordCodec() {}

    static void write(DataOutput out, Published p) throws IOException {
        writeTag(out, p.tag());
        writeContent(out, p.content());
        out.writeLong(p.atMillis());
        out.writeLong(p.delayMillis());
    }

    static void write(DataOutput out, Delivery d) throws IOException {
        out.writeByte(d.queue().ordinal());
        writeTag(out, d.tag());
        out.writeLong(d.messageId());
        out.writeInt(d.deliveryCount());
        writeContent(out, d.content());
        out.writeLong(d.pull().chunk());
        out.writeInt(d.pull().position());
        out.writeLong(d.pull().atMillis());
        out.writeLong(d.pull().leaseDeadlineMillis());
        out.writeByte(d.settle().action().ordinal());
        out.writeBoolean(d.settle().applied());
        out.writeLong(d.settle().atMillis());
        out.writeLong(d.settle().nackDelayMillis());
    }

    /** 다음 발행 기록. 파일 끝(또는 잘린 기록)이면 null. */
    static Published readPublished(DataInput in) throws IOException {
        try {
            Tag tag = readTag(in);
            Content content = readContent(in);
            return new Published(tag, content, in.readLong(), in.readLong());
        } catch (EOFException e) {
            return null;
        }
    }

    /** 다음 전달 기록. 파일 끝(또는 잘린 기록)이면 null. */
    static Delivery readDelivery(DataInput in) throws IOException {
        try {
            QueueKind queue = QUEUES[in.readUnsignedByte()];
            Tag tag = readTag(in);
            long messageId = in.readLong();
            int deliveryCount = in.readInt();
            Content content = readContent(in);
            Pull pull = new Pull(in.readLong(), in.readInt(), in.readLong(), in.readLong());
            Action action = ACTIONS[in.readUnsignedByte()];
            Settle settle = new Settle(action, in.readBoolean(), in.readLong(), in.readLong());
            return new Delivery(queue, tag, messageId, deliveryCount, content, pull, settle);
        } catch (EOFException e) {
            return null;
        }
    }

    private static void writeTag(DataOutput out, Tag t) throws IOException {
        out.writeLong(t.seed());
        out.writeLong(t.seq());
    }

    private static Tag readTag(DataInput in) throws IOException {
        return new Tag(in.readLong(), in.readLong());
    }

    private static void writeContent(DataOutput out, Content c) throws IOException {
        out.writeLong(c.payloadChecksum());
        out.writeInt(c.headers().size());
        for (Map.Entry<String, String> e : c.headers().entrySet()) {
            out.writeUTF(e.getKey());
            out.writeUTF(e.getValue());
        }
    }

    private static Content readContent(DataInput in) throws IOException {
        long checksum = in.readLong();
        int n = in.readInt();
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            headers.put(in.readUTF(), in.readUTF());
        }
        return new Content(checksum, headers);
    }
}
