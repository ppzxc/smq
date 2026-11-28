package io.github.ppzxc.smq;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

public final class StopAfterTimeout implements ExecutionCondition, TestWatcher {

    private static final AtomicBoolean TIMED_OUT = new AtomicBoolean();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return TIMED_OUT.get()
                ? ConditionEvaluationResult.disabled("앞서 시간 제한에 걸린 테스트가 있어 건너뛴다")
                : ConditionEvaluationResult.enabled("시간 제한에 걸린 테스트가 아직 없다");
    }

    @Override
    public void testFailed(ExtensionContext context, final Throwable cause) {
        if (cause instanceof TimeoutException) {
            TIMED_OUT.set(true);
        }
    }
}
