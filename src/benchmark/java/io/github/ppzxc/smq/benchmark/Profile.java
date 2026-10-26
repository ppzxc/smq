package io.github.ppzxc.smq.benchmark;

/** 스펙의 프로파일. 발행 배치 크기는 청크 크기와 같다. */
enum Profile {
    P0(256, 1),
    P1(256, 100),
    P2(2048, 300),
    P3(6144, 500),
    /** 스펙 표 밖에 더한 4KB 지점. 절대 하한은 없고 확장성과 회귀만 본다. */
    P4(4096, 400),
    /** 단건(청크 1) 짝. 데이터 크기는 P2·P3·P4와 같고 발행·꺼내기·ACK를 한 건씩 한다. 256B의 짝은 P0이다. */
    P2S(2048, 1),
    P3S(6144, 1),
    P4S(4096, 1);

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
