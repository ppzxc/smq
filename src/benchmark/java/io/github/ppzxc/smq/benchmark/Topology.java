package io.github.ppzxc.smq.benchmark;

/** S3에서 프로듀서와 컨슈머가 어디에 있는지. smq는 한 JVM만 열 수 있으므로(ADR-0012) 같은 프로세스뿐이다. S1과 S2에서는 쓰지 않는다. */
enum Topology {
    NONE(""),
    SAME_PROCESS("same-process");

    private final String id;

    Topology(String id) {
        this.id = id;
    }

    String id() {
        return id;
    }
}
