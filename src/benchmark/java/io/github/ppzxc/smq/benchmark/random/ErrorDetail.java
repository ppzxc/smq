package io.github.ppzxc.smq.benchmark.random;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link Verdict#ERROR}의 원인. 문자열 하나로 뭉개지 않고 어디서 났는지와 스택 트레이스 전체를 남긴다.
 *
 * @param location {@link #HARNESS}(하네스 JVM). 이전 기록과 형식을 맞추려고 문자열로 남겼다
 * @param stackTrace {@code Throwable.printStackTrace} 형식의 전체. 원인 사슬을 포함한다
 */
record ErrorDetail(String location, String exceptionClass, String message, String stackTrace) {

    static final String HARNESS = "harness";

    static ErrorDetail of(String location, Throwable t) {
        return new ErrorDetail(location, t.getClass().getName(), t.getMessage(), stackTraceOf(t));
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
        return m;
    }

    /** 사람이 읽는 여러 줄 형식(콘솔과 {@code violations.txt}). */
    String describe() {
        StringBuilder b = new StringBuilder();
        b.append("errorLocation=").append(location).append('\n');
        b.append("errorClass=").append(exceptionClass).append('\n');
        b.append("errorMessage=").append(message).append('\n');
        b.append("stackTrace:\n").append(stackTrace);
        return b.toString();
    }
}
