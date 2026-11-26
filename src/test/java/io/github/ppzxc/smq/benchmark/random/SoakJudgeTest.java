package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SoakJudgeTest {

    private static final long INTERVAL_MS = 500;

    private static final long CAP = 50_000;

    private static List<Long> depths(long start, double perSecond, int n) {
        List<Long> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(start + Math.round(perSecond * i * INTERVAL_MS / 1000.0));
        }
        return out;
    }

    @Test
    void 목표만큼_보내고_깊이가_일정하면_정상이다() {
        SoakJudge.Report r = SoakJudge.judge(500, 10.0, 5_000, 4_990, depths(100, 0, 20), INTERVAL_MS, CAP);
        assertEquals(1.0, r.achievedRatio(), 1e-9);
        assertEquals(499.0, r.ackRate(), 1e-9);
        assertEquals(0.0, r.backlogSlopePerSec(), 1e-9);
        assertEquals(100, r.maxDepth());
        assertFalse(r.backlogGrowing());
    }

    @Test
    void 목표에_못_미친_달성률은_기록만_하고_백로그_판정과_무관하다() {
        SoakJudge.Report r = SoakJudge.judge(500, 10.0, 1_000, 1_000, depths(0, 0, 20), INTERVAL_MS, CAP);
        assertEquals(0.2, r.achievedRatio(), 1e-9);
        assertFalse(r.backlogGrowing(), "달성률이 낮아도 백로그가 늘지 않으면 실패가 아니다");
    }

    @Test
    void 깊이가_계속_늘면_백로그_증가다() {

        SoakJudge.Report r = SoakJudge.judge(500, 10.0, 5_000, 3_000, depths(0, 200, 20), INTERVAL_MS, CAP);
        assertEquals(200.0, r.backlogSlopePerSec(), 1.0);
        assertEquals(1_900, r.maxDepth());
        assertTrue(r.backlogGrowing());
    }

    @Test
    void 기울기가_작거나_늘어난_양이_작으면_증가로_보지_않는다() {

        assertFalse(SoakJudge.judge(500, 10.0, 5_000, 4_800, depths(0, 20, 20), INTERVAL_MS, CAP)
                .backlogGrowing());

        assertFalse(SoakJudge.judge(500, 1.0, 500, 400, depths(0, 800, 3), INTERVAL_MS, CAP)
                .backlogGrowing());

        assertTrue(SoakJudge.judge(500, 1.0, 500, 400, depths(0, 800, 4), INTERVAL_MS, CAP)
                .backlogGrowing());
    }

    @Test
    void 표본이_3개_미만이면_추세를_판정하지_않는다() {
        SoakJudge.Report r = SoakJudge.judge(500, 1.0, 500, 500, depths(0, 5_000, 2), INTERVAL_MS, CAP);
        assertEquals(0.0, r.backlogSlopePerSec(), 1e-9);
        assertFalse(r.backlogGrowing());
    }

    @Test
    void 표본이_없으면_최대_깊이는_0이다() {
        SoakJudge.Report r = SoakJudge.judge(500, 10.0, 0, 0, List.of(), INTERVAL_MS, CAP);
        assertEquals(0, r.maxDepth());
        assertEquals(0.0, r.achievedRatio(), 1e-9);
    }

    @Test
    void 프로듀서가_쉬는_깊이까지_찼으면_추세가_평평해도_백로그_증가다() {

        List<Long> plateau = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            plateau.add(CAP);
        }
        SoakJudge.Report r = SoakJudge.judge(500, 10.0, 2_000, 1_000, plateau, INTERVAL_MS, CAP);
        assertEquals(0.0, r.backlogSlopePerSec(), 1e-9);
        assertTrue(r.backlogGrowing());
    }

    @Test
    void 상한의_90퍼센트_미만이면_포화로_보지_않는다() {
        List<Long> below = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            below.add(CAP * 89 / 100);
        }
        assertFalse(SoakJudge.judge(500, 10.0, 5_000, 5_000, below, INTERVAL_MS, CAP)
                .backlogGrowing());
        below.set(7, CAP * 90 / 100);
        assertTrue(SoakJudge.judge(500, 10.0, 5_000, 5_000, below, INTERVAL_MS, CAP)
                .backlogGrowing());
    }
}
