package io.github.ppzxc.smq;

public record QueueCounts(long ready, long leased, long delayed) {
}
