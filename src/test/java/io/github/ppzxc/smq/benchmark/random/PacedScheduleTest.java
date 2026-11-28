package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PacedScheduleTest {

    private static final long SEC = 1_000_000_000L;

    @Test
    @DisplayName("첫 배치는 바로 나가고 다음은 배치 크기를 속도로 나눈 간격이다 — first Batch Is Due Immediately")
    void firstBatchIsDueImmediately() {
        final PacedSchedule s;
        s = new PacedSchedule(500, 100, 0);
        assertEquals(0, s.nextDueNanos(), "Next due nanos should match the expected value.");
        s.sent();
        s.sent();
    }

    @Test
    @DisplayName("첫 배치는 바로 나가고 다음은 배치 크기를 속도로 나눈 간격이다 — second Batch Is Due After One Interval")
    void secondBatchIsDueAfterOneInterval() {
        final PacedSchedule s;
        s = new PacedSchedule(500, 100, 0);
        s.sent();
        assertEquals(SEC / 5, s.nextDueNanos(), "100개를 500/s로 보내면 0.2초 간격");
        s.sent();
    }

    @Test
    @DisplayName("첫 배치는 바로 나가고 다음은 배치 크기를 속도로 나눈 간격이다 — third Batch Is Due After Two Intervals")
    void thirdBatchIsDueAfterTwoIntervals() {
        final PacedSchedule s;
        s = new PacedSchedule(500, 100, 0);
        s.sent();
        s.sent();
        assertEquals(2 * SEC / 5, s.nextDueNanos(), "Next due nanos should match the expected value.");
    }

    @Test
    @DisplayName("시작 시각이 기준이다 — first Batch Uses Start Time")
    void firstBatchUsesStartTime() {
        final PacedSchedule s;
        s = new PacedSchedule(1000, 10, 7 * SEC);
        assertEquals(7 * SEC, s.nextDueNanos(), "Next due nanos should match the expected value.");
        s.sent();
    }

    @Test
    @DisplayName("시작 시각이 기준이다 — second Batch Uses Start Time Plus Interval")
    void secondBatchUsesStartTimePlusInterval() {
        final PacedSchedule s;
        s = new PacedSchedule(1000, 10, 7 * SEC);
        s.sent();
        assertEquals(7 * SEC + SEC / 100, s.nextDueNanos(), "Next due nanos should match the expected value.");
    }

    @Test
    @DisplayName("늦어져도 기준은 밀리지 않아서 따라잡는다 — delayed Schedule Keeps Original Deadlines")
    void delayedScheduleKeepsOriginalDeadlines() {
        final PacedSchedule s;
        s = new PacedSchedule(500, 100, 0);
        s.sent();
        s.sent();
        s.sent();
        assertEquals(3 * SEC / 5, s.nextDueNanos(), "Next due nanos should match the expected value.");
    }

    @Test
    @DisplayName("늦어져도 기준은 밀리지 않아서 따라잡는다 — delayed Schedule Does Not Wait")
    void delayedScheduleDoesNotWait() {
        final PacedSchedule s;
        s = new PacedSchedule(500, 100, 0);
        s.sent();
        s.sent();
        s.sent();
        assertEquals(0, s.waitNanos(10 * SEC), "늦었으면 기다리지 않는다");
    }

    @Test
    @DisplayName("기다릴 시간은 다음 시각까지이고 지났으면 0이다 — waiting Stops At Next Deadline")
    void waitingStopsAtNextDeadline() {
        final PacedSchedule s;
        s = new PacedSchedule(500, 100, 0);
        s.sent();
        assertEquals(SEC / 10, s.waitNanos(SEC / 10), "Wait nanos should match the expected value.");
    }

    @Test
    @DisplayName("기다릴 시간은 다음 시각까지이고 지났으면 0이다 — waiting Is Zero After Deadline")
    void waitingIsZeroAfterDeadline() {
        final PacedSchedule s;
        s = new PacedSchedule(500, 100, 0);
        s.sent();
        assertEquals(0, s.waitNanos(SEC), "Wait nanos should match the expected value.");
    }

    @Test
    @DisplayName("속도와 배치는 양수여야 한다 — pacedScheduleRejectsZeroRate")
    void pacedScheduleRejectsZeroRate() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PacedSchedule(0, 10, 0),
                "Construction should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도와 배치는 양수여야 한다 — pacedScheduleRejectsZeroBatch")
    void pacedScheduleRejectsZeroBatch() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PacedSchedule(10, 0, 0),
                "Construction should throw IllegalArgumentException.");
    }
}
