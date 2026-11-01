package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 달성률과 백로그 추세 판정. 표본 목록만 받는 순수 로직이다. */
class SoakJudgeTest {

    private static final long INTERVAL_MS = 500;
    /** 프로듀서가 쉬기 시작하는 깊이. 깊이가 여기서 멈추면 추세가 평평해 보인다. */
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
        // 초당 +200개씩, 10초 동안 약 2,000개 증가. 목표 속도의 5%(25/s)와 2초 분량(1,000개)을 모두 넘는다.
        SoakJudge.Report r = SoakJudge.judge(500, 10.0, 5_000, 3_000, depths(0, 200, 20), INTERVAL_MS, CAP);
        assertEquals(200.0, r.backlogSlopePerSec(), 1.0);
        assertEquals(1_900, r.maxDepth());
        assertTrue(r.backlogGrowing());
    }

    @Test
    void 기울기가_작거나_늘어난_양이_작으면_증가로_보지_않는다() {
        // 기울기는 20/s로 5%(25/s) 아래다.
        assertFalse(SoakJudge.judge(500, 10.0, 5_000, 4_800, depths(0, 20, 20), INTERVAL_MS, CAP)
                .backlogGrowing());
        // 기울기는 800/s로 크지만 표본 구간이 1초라 늘어난 양(800개)이 2초 분량(1,000개)보다 작다.
        assertFalse(SoakJudge.judge(500, 1.0, 500, 400, depths(0, 800, 3), INTERVAL_MS, CAP)
                .backlogGrowing());
        // 같은 기울기라도 구간이 길어 늘어난 양이 2초 분량을 넘으면 증가다.
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
        // 소비가 못 따라가 깊이가 상한에서 멈춘 경우. 기울기만 보면 0이라 놓친다.
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
