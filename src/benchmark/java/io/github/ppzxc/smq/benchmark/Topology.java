package io.github.ppzxc.smq.benchmark;

/** S3에서 프로듀서와 컨슈머가 같은 JVM에 있는지, 서로 다른 JVM에 있는지. S1과 S2에서는 쓰지 않는다. */
enum Topology {
    NONE(""),
    SAME_PROCESS("same-process"),
    CROSS_PROCESS("cross-process");

    private final String id;

    Topology(String id) {
        this.id = id;
    }

    String id() {
        return id;
    }
}
