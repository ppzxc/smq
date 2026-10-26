package io.github.ppzxc.smq;

public record QueueOptions() {

    public static QueueOptions defaults() {
        return new QueueOptions();
    }
}
