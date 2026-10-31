package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** 목표 속도에 맞춘 발행 시각 계산. 시계를 읽지 않는 순수 로직이라 정확한 값으로 확인한다. */
class PacedScheduleTest {

    private static final long SEC = 1_000_000_000L;

    @Test
    void 첫_배치는_바로_나가고_다음은_배치_크기를_속도로_나눈_간격이다() {
        PacedSchedule s = new PacedSchedule(500, 100, 0);
        assertEquals(0, s.nextDueNanos());
        s.sent();
        assertEquals(SEC / 5, s.nextDueNanos(), "100개를 500/s로 보내면 0.2초 간격");
        s.sent();
        assertEquals(2 * SEC / 5, s.nextDueNanos());
    }

    @Test
    void 시작_시각이_기준이다() {
        PacedSchedule s = new PacedSchedule(1000, 10, 7 * SEC);
        assertEquals(7 * SEC, s.nextDueNanos());
        s.sent();
        assertEquals(7 * SEC + SEC / 100, s.nextDueNanos());
    }

    @Test
    void 늦어져도_기준은_밀리지_않아서_따라잡는다() {
        PacedSchedule s = new PacedSchedule(500, 100, 0);
        s.sent();
        s.sent();
        s.sent();
        // 10초 늦게 와도 다음 시각은 원래 격자 위(0.6초)이므로 밀린 만큼 연달아 나간다.
        assertEquals(3 * SEC / 5, s.nextDueNanos());
        assertEquals(10 * SEC - 3 * SEC / 5, s.lagNanos(10 * SEC));
    }

    @Test
    void 늦지_않았으면_지연은_0이다() {
        PacedSchedule s = new PacedSchedule(500, 100, 0);
        s.sent();
        assertEquals(0, s.lagNanos(SEC / 10));
        assertEquals(SEC / 10, s.waitNanos(SEC / 10));
        assertEquals(0, s.waitNanos(SEC));
    }

    @Test
    void 속도와_배치는_양수여야_한다() {
        assertThrows(IllegalArgumentException.class, () -> new PacedSchedule(0, 10, 0));
        assertThrows(IllegalArgumentException.class, () -> new PacedSchedule(10, 0, 0));
    }
}
