package io.github.ppzxc.smq.benchmark;

/** 측정 대상 저장소. 목표는 SQLITE에만 있고, MEMORY는 코어 오버헤드를 보는 진단값이다. */
enum Storage {
    SQLITE("sqlite"),
    MEMORY("memory");

    private final String id;

    Storage(String id) {
        this.id = id;
    }

    String id() {
        return id;
    }
}
