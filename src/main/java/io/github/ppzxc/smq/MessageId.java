package io.github.ppzxc.smq;

public record MessageId(long value) implements Comparable<MessageId> {

    @Override
    public int compareTo(final MessageId other) {
        return Long.compare(value, other.value);
    }
}
