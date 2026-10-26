package io.github.ppzxc.smq;

public enum Outcome {
    OK,
    /** 낡은 연산: 이미 정산됐거나 다른 리스가 가져간 메시지. */
    STALE
}
