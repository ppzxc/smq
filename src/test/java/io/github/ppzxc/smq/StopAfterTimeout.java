package io.github.ppzxc.smq;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

/**
 * 시간 제한에 한 번 걸리면 나머지 테스트는 건너뛴다. 재시도 루프가 깨졌다면 테스트마다 제한 시간을 다 써서
 * 전체가 끝나지 않으므로, 이름이 남는 첫 실패만 남기고 멈춘다. {@code junit-platform.properties}가 자동 등록한다.
 */
public final class StopAfterTimeout implements ExecutionCondition, TestWatcher {

    private static final AtomicBoolean TIMED_OUT = new AtomicBoolean();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return TIMED_OUT.get()
                ? ConditionEvaluationResult.disabled("앞서 시간 제한에 걸린 테스트가 있어 건너뛴다")
                : ConditionEvaluationResult.enabled("시간 제한에 걸린 테스트가 아직 없다");
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        if (cause instanceof TimeoutException) {
            TIMED_OUT.set(true);
        }
    }
}
