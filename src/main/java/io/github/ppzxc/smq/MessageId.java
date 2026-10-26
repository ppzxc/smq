package io.github.ppzxc.smq;

/** 큐 안에서만 유일한 메시지 id. 순서가 FIFO 순서다. */
public record MessageId(long value) implements Comparable<MessageId> {

    @Override
    public int compareTo(MessageId other) {
        return Long.compare(value, other.value);
    }
}
