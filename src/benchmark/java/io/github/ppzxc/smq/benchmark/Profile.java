package io.github.ppzxc.smq.benchmark;

/**
 * 벤치 프로파일. 발행 배치 크기는 청크 크기와 같다. 번호는 데이터 크기 순이고, 같은 크기에서 짝수가 단건(청크 1), 홀수가 묶음이다.
 */
enum Profile {
    P0(256, 1),
    P1(256, 100),
    P2(2048, 1),
    P3(2048, 300),
    P4(4096, 1),
    /** 스펙 표 밖에 더한 4KB 묶음 지점. 절대 하한은 없고 회귀만 본다. */
    P5(4096, 400),
    P6(6144, 1),
    P7(6144, 500);

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
