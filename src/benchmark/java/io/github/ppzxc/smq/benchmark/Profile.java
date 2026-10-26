package io.github.ppzxc.smq.benchmark;

/** 스펙의 프로파일. 발행 배치 크기는 청크 크기와 같다. */
enum Profile {
    P0(256, 1),
    P1(256, 100),
    P2(2048, 300),
    P3(6144, 500);

    private final int payloadBytes;
    private final int chunk;

    Profile(int payloadBytes, int chunk) {
        this.payloadBytes = payloadBytes;
        this.chunk = chunk;
    }

    int payloadBytes() {
        return payloadBytes;
    }

    /** 청크 크기이자 발행 배치 크기. */
    int chunk() {
        return chunk;
    }
}
