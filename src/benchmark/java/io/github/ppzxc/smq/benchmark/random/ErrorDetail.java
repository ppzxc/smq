package io.github.ppzxc.smq.benchmark.random;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link Verdict#ERROR}의 원인. 문자열 하나로 뭉개지 않고 어디서 났는지와 스택 트레이스 전체를 남긴다.
 *
 * @param location {@link #HARNESS}(하네스 JVM) 또는 {@code child-<번호>}(그 번호의 자식 JVM)
 * @param stackTrace {@code Throwable.printStackTrace} 형식의 전체. 원인 사슬을 포함한다
 * @param exitCode 자식 JVM이 비정상 종료했을 때의 종료 코드. 알 수 없거나 자식이 아니면 null
 * @param stderrTail 자식 JVM 표준 오류의 끝부분(상한 있음). 자식이 아니면 null
 */
record ErrorDetail(
        String location,
        String exceptionClass,
        String message,
        String stackTrace,
        Integer exitCode,
        String stderrTail) {

    static final String HARNESS = "harness";

    static String child(int process) {
        return "child-" + process;
    }

    static ErrorDetail of(String location, Throwable t, Integer exitCode, String stderrTail) {
        return new ErrorDetail(location, t.getClass().getName(), t.getMessage(), stackTraceOf(t), exitCode, stderrTail);
    }

    static String stackTraceOf(Throwable t) {
        StringWriter w = new StringWriter();
        t.printStackTrace(new PrintWriter(w));
        return w.toString();
    }

    Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("location", location);
        m.put("exceptionClass", exceptionClass);
        m.put("message", message);
        m.put("stackTrace", stackTrace);
        m.put("exitCode", exitCode);
        m.put("stderrTail", stderrTail);
        return m;
    }

    /** 사람이 읽는 여러 줄 형식(콘솔과 {@code violations.txt}). */
    String describe() {
        StringBuilder b = new StringBuilder();
        b.append("errorLocation=").append(location).append('\n');
        b.append("errorClass=").append(exceptionClass).append('\n');
        b.append("errorMessage=").append(message).append('\n');
        if (exitCode != null) {
            b.append("exitCode=").append(exitCode).append('\n');
        }
        if (stderrTail != null && !stderrTail.isEmpty()) {
            b.append("stderrTail:\n").append(stderrTail).append('\n');
        }
        b.append("stackTrace:\n").append(stackTrace);
        return b.toString();
    }
}
