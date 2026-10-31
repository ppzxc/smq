package io.github.ppzxc.smq;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * 연장 결과. {@code leaseDeadline}은 요청한 새 기한이고, 메시지가 없는 빈 청크를 연장하면 null이다. 기한은 처리한 시각으로
 * 계산한 값이며, 실제로 반영된 것은 {@code outcomes}가 {@code OK}인 메시지뿐이다.
 */
public record Extension(@Nullable Instant leaseDeadline, Outcomes outcomes) {}
