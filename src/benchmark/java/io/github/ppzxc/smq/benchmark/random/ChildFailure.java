package io.github.ppzxc.smq.benchmark.random;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 자식 JVM 하나가 실패했다는 것. 부모가 어느 자식인지, 종료 코드와 표준 오류 끝부분을 함께 들고 결과의 {@link ErrorDetail}로 옮긴다.
 */
final class ChildFailure extends Exception {

    /** 표준 오류에서 남기는 끝부분의 상한(바이트). */
    static final int STDERR_TAIL_BYTES = 8 * 1024;

    private final int process;
    private final Integer exitCode;
    private final String stderrTail;

    /**
     * @param exitCode 알 수 없으면 null(제한 시간 초과로 우리가 죽인 경우 등)
     * @param stderrTail 표준 오류의 끝부분. 없으면 빈 문자열
     */
    ChildFailure(int process, Integer exitCode, String stderrTail, Throwable cause) {
        super("자식 JVM " + process + " 실패" + (exitCode == null ? "" : ", 종료 코드 " + exitCode), cause);
        this.process = process;
        this.exitCode = exitCode;
        this.stderrTail = stderrTail;
    }

    int process() {
        return process;
    }

    Integer exitCode() {
        return exitCode;
    }

    String stderrTail() {
        return stderrTail;
    }

    /** {@code file}의 마지막 {@code maxBytes}바이트 안쪽 문자열. 파일이 없으면 빈 문자열. */
    static String stderrTail(Path file, int maxBytes) {
        if (!Files.isRegularFile(file)) {
            return "";
        }
        try (RandomAccessFile f = new RandomAccessFile(file.toFile(), "r")) {
            long len = f.length();
            int n = (int) Math.min(len, maxBytes);
            byte[] bytes = new byte[n];
            f.seek(len - n);
            f.readFully(bytes);
            String s = new String(bytes, StandardCharsets.UTF_8);
            // 잘린 맨 앞의 깨진 문자(대체 문자)는 버린다.
            s = s.startsWith("�") ? s.substring(1) : s;
            // UTF-8 바이트 상한이 문자 수 상한보다 크므로 문자 수도 맞춘다.
            return s.length() > maxBytes ? s.substring(s.length() - maxBytes) : s;
        } catch (IOException e) {
            return "(표준 오류를 읽지 못했다: " + e + ")";
        }
    }
}
