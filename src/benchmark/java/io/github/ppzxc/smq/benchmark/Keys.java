package io.github.ppzxc.smq.benchmark;

/** 결과와 기준선 파일이 쓰는 키 형식. 비교기와 집계기가 같은 곳에서 키를 만든다. */
final class Keys {

    private Keys() {}

    static String of(
            final Storage storage,
            final Scenario scenario,
            final Profile profile,
            final String durability,
            final int depth) {
        return storage.id() + "/" + scenario + "/" + profile + "/" + durability + "/" + depthId(depth);
    }

    static String depthId(final int depth) {
        if (depth == 0) return "empty";
        if (depth == RunSpec.DEEP) return "1m";
        return String.valueOf(depth);
    }

    /** SQLite의 처리량 항목: S1/S2. */
    static String throughput(final String key) {
        return key + ":msgPerSec";
    }

    static String p50(final String key) {
        return key + ":p50Ms";
    }

    static String p99(final String key) {
        return key + ":p99Ms";
    }
}
