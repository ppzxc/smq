package io.github.ppzxc.smq.storage;

/** 꺼낼 때마다 새로 발급하는 리스 토큰. 저장소 안에서 재사용하지 않고, SPI 밖에서는 불투명하다. */
public record LeaseToken(long value) {
}
