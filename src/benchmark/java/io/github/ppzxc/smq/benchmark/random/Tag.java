package io.github.ppzxc.smq.benchmark.random;

/** 프로듀서가 메시지마다 붙이는 식별 태그. 시드와 프로세스 안에서 순번이 유일하다. */
record Tag(long seed, int process, long seq) {

    @Override
    public String toString() {
        return seed + "/" + process + "/" + seq;
    }
}
