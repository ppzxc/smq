package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** 크기가 고정된 지연 히스토그램. 백분위수는 표본을 모두 정렬한 nearest-rank와 같아야 한다. */
class LatencyHistogramTest {

    @Test
    void 백분위수는_nearest_rank다() {
        LatencyHistogram h = new LatencyHistogram();
        for (long v = 1; v <= 100; v++) {
            h.add(v * 10);
        }
        assertEquals(500, h.percentile(50));
        assertEquals(990, h.percentile(99));
        assertEquals(1_000, h.percentile(100));
        assertEquals(100, h.total());
    }

    @Test
    void 한계를_넘는_값은_한계로_세고_음수는_0으로_센다() {
        LatencyHistogram h = new LatencyHistogram();
        h.add(-5);
        h.add(Long.MAX_VALUE);
        assertEquals(0, h.percentile(50));
        assertEquals(LatencyHistogram.MAX_MILLIS, h.percentile(100));
    }

    @Test
    void 표본이_없으면_백분위수를_거부한다() {
        assertThrows(IllegalStateException.class, () -> new LatencyHistogram().percentile(50));
    }
}
