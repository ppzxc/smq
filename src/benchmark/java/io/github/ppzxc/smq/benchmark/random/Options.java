package io.github.ppzxc.smq.benchmark.random;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;

/**
 * 명령행 옵션.
 *
 * <ul>
 *   <li>{@code --seed <n>}: 그 시드 하나만 돈다.
 *   <li>{@code --budget <기간>}: 새 실행을 시작할 수 있는 세션 시간. {@code 30m}, {@code 90s}, {@code 2h}, {@code PT30M}. 기본 30분.
 *   <li>{@code --master-seed <n>}: 실행 시드를 뽑을 마스터 시드. 기본은 현재 시각.
 *   <li>{@code --load <기간>}: 부하 구간 길이. 기본 10초.
 *   <li>{@code --smoke}: 고정 시드 {@link #SMOKE_SEEDS}를 실행당 2초씩 돈다(미리 채운 깊이와 리스 기한을 줄인 한도).
 *   <li>{@code --heap-dump}: 자식 JVM에 {@code -XX:+HeapDumpOnOutOfMemoryError}를 주고 덤프를 실행 디렉터리에 남긴다. 기본은 끈다(덤프가 크다).
 *   <li>{@code --out-dir <디렉터리>}: 산출물 위치. 기본 {@code build/random-bench}.
 * </ul>
 */
record Options(OptionalLong seed, Duration budget, long masterSeed, Duration load, boolean smoke, Path outDir,
        boolean heapDump) {

    static final Duration DEFAULT_BUDGET = Duration.ofMinutes(30);
    static final Duration SMOKE_LOAD = Duration.ofSeconds(2);
    static final List<Long> SMOKE_SEEDS = List.of(1L, 2L, 3L, 4L, 5L, 6L);

    static Options parse(String[] args, long nowMillis) {
        OptionalLong seed = OptionalLong.empty();
        Duration budget = DEFAULT_BUDGET;
        long master = nowMillis;
        Duration load = null;
        boolean smoke = false;
        boolean heapDump = false;
        Path out = Path.of("build/random-bench");
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.equals("--smoke")) {
                smoke = true;
                continue;
            }
            if (a.equals("--heap-dump")) {
                heapDump = true;
                continue;
            }
            if (!a.startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("잘못된 인자: " + a);
            }
            String v = args[++i];
            switch (a) {
                case "--seed" -> seed = OptionalLong.of(parseLong(a, v));
                case "--budget" -> budget = parseDuration(v);
                case "--master-seed" -> master = parseLong(a, v);
                case "--load" -> load = parseDuration(v);
                case "--out-dir" -> out = Path.of(v);
                default -> throw new IllegalArgumentException("알 수 없는 옵션: " + a);
            }
        }
        if (load == null) {
            load = smoke ? SMOKE_LOAD : Duration.ofMillis(Limits.DEFAULT_LOAD_MILLIS);
        }
        if (smoke && seed.isPresent()) {
            throw new IllegalArgumentException("--smoke와 --seed는 함께 쓸 수 없다");
        }
        return new Options(seed, budget, master, load, smoke, out.toAbsolutePath(), heapDump);
    }

    private static long parseLong(String option, String v) {
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(option + "는 정수여야 한다: " + v);
        }
    }

    /** {@code 30m}, {@code 90s}, {@code 2h}, {@code 1500ms}, {@code PT30M}. 단위가 없으면 초. */
    static Duration parseDuration(String v) {
        String s = v.trim();
        try {
            if (s.regionMatches(true, 0, "PT", 0, 2)) {
                return Duration.parse(s);
            }
            if (s.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
            }
            long n = Long.parseLong(s.substring(0, s.length() - (Character.isDigit(s.charAt(s.length() - 1)) ? 0 : 1)));
            return switch (Character.toLowerCase(s.charAt(s.length() - 1))) {
                case 'h' -> Duration.ofHours(n);
                case 'm' -> Duration.ofMinutes(n);
                default -> Duration.ofSeconds(n);
            };
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("기간을 읽을 수 없다: " + v);
        }
    }
}
