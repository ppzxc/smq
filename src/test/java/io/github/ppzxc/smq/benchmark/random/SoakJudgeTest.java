package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SoakJudgeTest {

    private static final long INTERVAL_MS = 500;

    private static final long CAP = 50_000;

    private static List<Long> depths(final long start, final double perSecond, final int n) {
        final List<Long> out;
        out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(start + Math.round(perSecond * i * INTERVAL_MS / 1000.0));
        }
        return out;
    }

    @Test
    @DisplayName("목표만큼 보내고 깊이가 일정하면 정상이다 — flat Backlog Reports Achieved Ratio")
    void flatBacklogReportsAchievedRatio() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 5_000, 4_990, depths(100, 0, 20), INTERVAL_MS, CAP);
        assertEquals(1.0, r.achievedRatio(), 1e-9, "Achieved ratio should match the expected value.");
    }

    @Test
    @DisplayName("목표만큼 보내고 깊이가 일정하면 정상이다 — flat Backlog Reports Ack Rate")
    void flatBacklogReportsAckRate() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 5_000, 4_990, depths(100, 0, 20), INTERVAL_MS, CAP);
        assertEquals(499.0, r.ackRate(), 1e-9, "Ack rate should match the expected value.");
    }

    @Test
    @DisplayName("목표만큼 보내고 깊이가 일정하면 정상이다 — flat Backlog Reports Zero Slope")
    void flatBacklogReportsZeroSlope() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 5_000, 4_990, depths(100, 0, 20), INTERVAL_MS, CAP);
        assertEquals(0.0, r.backlogSlopePerSec(), 1e-9, "Backlog slope per sec should match the expected value.");
    }

    @Test
    @DisplayName("목표만큼 보내고 깊이가 일정하면 정상이다 — flat Backlog Reports Maximum Depth")
    void flatBacklogReportsMaximumDepth() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 5_000, 4_990, depths(100, 0, 20), INTERVAL_MS, CAP);
        assertEquals(100, r.maxDepth(), "Max depth should match the expected value.");
    }

    @Test
    @DisplayName("목표만큼 보내고 깊이가 일정하면 정상이다 — flat Backlog Does Not Grow")
    void flatBacklogDoesNotGrow() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 5_000, 4_990, depths(100, 0, 20), INTERVAL_MS, CAP);
        assertFalse(r.backlogGrowing(), "The backlog should not be growing.");
    }

    @Test
    @DisplayName("목표에 못 미친 달성률은 기록만 하고 백로그 판정과 무관하다 — low Achievement Is Recorded")
    void lowAchievementIsRecorded() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 1_000, 1_000, depths(0, 0, 20), INTERVAL_MS, CAP);
        assertEquals(0.2, r.achievedRatio(), 1e-9, "Achieved ratio should match the expected value.");
    }

    @Test
    @DisplayName("목표에 못 미친 달성률은 기록만 하고 백로그 판정과 무관하다 — low Achievement Does Not Imply Backlog Growth")
    void lowAchievementDoesNotImplyBacklogGrowth() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 1_000, 1_000, depths(0, 0, 20), INTERVAL_MS, CAP);
        assertFalse(r.backlogGrowing(), "달성률이 낮아도 백로그가 늘지 않으면 실패가 아니다");
    }

    @Test
    @DisplayName("깊이가 계속 늘면 백로그 증가다 — increasing Backlog Reports Slope")
    void increasingBacklogReportsSlope() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 5_000, 3_000, depths(0, 200, 20), INTERVAL_MS, CAP);
        assertEquals(200.0, r.backlogSlopePerSec(), 1.0, "Backlog slope per sec should match the expected value.");
    }

    @Test
    @DisplayName("깊이가 계속 늘면 백로그 증가다 — increasing Backlog Reports Maximum Depth")
    void increasingBacklogReportsMaximumDepth() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 5_000, 3_000, depths(0, 200, 20), INTERVAL_MS, CAP);
        assertEquals(1_900, r.maxDepth(), "Max depth should match the expected value.");
    }

    @Test
    @DisplayName("깊이가 계속 늘면 백로그 증가다 — increasing Backlog Is Reported As Growth")
    void increasingBacklogIsReportedAsGrowth() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 5_000, 3_000, depths(0, 200, 20), INTERVAL_MS, CAP);
        assertTrue(r.backlogGrowing(), "The backlog should be growing.");
    }

    @Test
    @DisplayName("기울기가 작거나 늘어난 양이 작으면 증가로 보지 않는다 — small slope")
    void smallBacklogSlopeIsNotGrowth() {
        assertFalse(
                SoakJudge.judge(500, 10.0, 5_000, 4_800, depths(0, 20, 20), INTERVAL_MS, CAP)
                        .backlogGrowing(),
                "The backlog should not be growing.");
    }

    @Test
    @DisplayName("기울기가 작거나 늘어난 양이 작으면 증가로 보지 않는다 — small increase")
    void smallBacklogIncreaseIsNotGrowth() {
        assertFalse(
                SoakJudge.judge(500, 1.0, 500, 400, depths(0, 800, 3), INTERVAL_MS, CAP)
                        .backlogGrowing(),
                "The backlog should not be growing.");
    }

    @Test
    @DisplayName("기울기가 작거나 늘어난 양이 작으면 증가로 보지 않는다 — large increase")
    void largeBacklogIncreaseIsGrowth() {
        assertTrue(
                SoakJudge.judge(500, 1.0, 500, 400, depths(0, 800, 4), INTERVAL_MS, CAP)
                        .backlogGrowing(),
                "The backlog should be growing.");
    }

    @Test
    @DisplayName("표본이 3개 미만이면 추세를 판정하지 않는다 — insufficient Samples Report Zero Slope")
    void insufficientSamplesReportZeroSlope() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 1.0, 500, 500, depths(0, 5_000, 2), INTERVAL_MS, CAP);
        assertEquals(0.0, r.backlogSlopePerSec(), 1e-9, "Backlog slope per sec should match the expected value.");
    }

    @Test
    @DisplayName("표본이 3개 미만이면 추세를 판정하지 않는다 — insufficient Samples Do Not Report Growth")
    void insufficientSamplesDoNotReportGrowth() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 1.0, 500, 500, depths(0, 5_000, 2), INTERVAL_MS, CAP);
        assertFalse(r.backlogGrowing(), "The backlog should not be growing.");
    }

    @Test
    @DisplayName("표본이 없으면 최대 깊이는 0이다 — no Samples Report Zero Maximum Depth")
    void noSamplesReportZeroMaximumDepth() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 0, 0, List.of(), INTERVAL_MS, CAP);
        assertEquals(0, r.maxDepth(), "Max depth should match the expected value.");
    }

    @Test
    @DisplayName("표본이 없으면 최대 깊이는 0이다 — no Samples Report Zero Achievement")
    void noSamplesReportZeroAchievement() {
        final SoakJudge.Report r;
        r = SoakJudge.judge(500, 10.0, 0, 0, List.of(), INTERVAL_MS, CAP);
        assertEquals(0.0, r.achievedRatio(), 1e-9, "Achieved ratio should match the expected value.");
    }

    @Test
    @DisplayName("프로듀서가 쉬는 깊이까지 찼으면 추세가 평평해도 백로그 증가다 — saturated Backlog Reports Flat Slope")
    void saturatedBacklogReportsFlatSlope() {
        final List<Long> plateau;
        final SoakJudge.Report r;
        plateau = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            plateau.add(CAP);
        }
        r = SoakJudge.judge(500, 10.0, 2_000, 1_000, plateau, INTERVAL_MS, CAP);
        assertEquals(0.0, r.backlogSlopePerSec(), 1e-9, "Backlog slope per sec should match the expected value.");
    }

    @Test
    @DisplayName("프로듀서가 쉬는 깊이까지 찼으면 추세가 평평해도 백로그 증가다 — saturated Backlog Reports Growth")
    void saturatedBacklogReportsGrowth() {
        final List<Long> plateau;
        final SoakJudge.Report r;
        plateau = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            plateau.add(CAP);
        }
        r = SoakJudge.judge(500, 10.0, 2_000, 1_000, plateau, INTERVAL_MS, CAP);
        assertTrue(r.backlogGrowing(), "The backlog should be growing.");
    }

    @Test
    @DisplayName("상한의 90퍼센트 미만이면 포화로 보지 않는다 — below Ninety Percent Is Not Saturated")
    void belowNinetyPercentIsNotSaturated() {
        final List<Long> below;
        below = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            below.add(CAP * 89 / 100);
        }
        assertFalse(
                SoakJudge.judge(500, 10.0, 5_000, 5_000, below, INTERVAL_MS, CAP)
                        .backlogGrowing(),
                "The backlog should not be growing.");
        below.set(7, CAP * 90 / 100);
    }

    @Test
    @DisplayName("상한의 90퍼센트 미만이면 포화로 보지 않는다 — ninety Percent Is Saturated")
    void ninetyPercentIsSaturated() {
        final List<Long> below;
        below = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            below.add(CAP * 89 / 100);
        }
        below.set(7, CAP * 90 / 100);
        assertTrue(
                SoakJudge.judge(500, 10.0, 5_000, 5_000, below, INTERVAL_MS, CAP)
                        .backlogGrowing(),
                "The backlog should be growing.");
    }
}
