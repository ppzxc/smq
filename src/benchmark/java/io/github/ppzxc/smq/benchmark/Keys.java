package io.github.ppzxc.smq.benchmark;

/** 결과와 기준선 파일이 쓰는 키 형식. 비교기와 집계기가 같은 곳에서 키를 만든다. */
final class Keys {

    private Keys() {}

    static String of(Storage storage, Scenario scenario, Profile profile, String shape, String durability, int depth) {
        return storage.id() + "/" + scenario + "/" + profile + "/" + shape + "/" + durability + "/" + depthId(depth);
    }

    static String depthId(int depth) {
        if (depth == 0) return "empty";
        if (depth == RunSpec.DEEP) return "1m";
        return String.valueOf(depth);
    }

    /** SQLite의 처리량 항목: S1/S2. */
    static String throughput(String key) {
        return key + ":msgPerSec";
    }

    static String p50(String key) {
        return key + ":p50Ms";
    }

    static String p99(String key) {
        return key + ":p99Ms";
    }
}
