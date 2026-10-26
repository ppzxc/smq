package io.github.ppzxc.smq.storage;

import java.time.Instant;
import java.util.List;

/** 연장 결과: 새 기한과 메시지별 결과. */
public record Extended(Instant leaseDeadline, List<Result> results) {
}
