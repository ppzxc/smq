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
 *   <li>{@code --rate <n>}: 고정 속도 모드. 프로듀서 전체가 초당 n개를 보내고 달성률과 백로그 추세를 판정한다. JFR이 기본으로 켜진다.
 *   <li>{@code --message-bytes <n>}: 고정 속도 모드의 메시지 크기 상한(최소는 그 절반, 균등). 기본 1,024. {@code --rate}가 있어야 한다.
 *   <li>{@code --depth <n>}: 고정 속도 모드의 미리 채울 깊이(데이터 256MB 추정 상한 안에서). 기본 100,000. {@code --rate}가 있어야 한다.
 *   <li>{@code --standing-queue}: 고정 속도 모드에서 큐 깊이가 미리 채운 깊이 이하면 컨슈머가 쉬어서 그 깊이가 유지되게 한다. {@code --rate}가 있어야 한다.
 *   <li>{@code --jfr}: JVM에 Java Flight Recorder 연속 레코딩을 켠다. 실행 디렉터리에 {@code .jfr} 파일을 남긴다.
 *   <li>{@code --out-dir <디렉터리>}: 산출물 위치. 기본 {@code build/random-bench}.
 * </ul>
 */
record Options(
        OptionalLong seed,
        Duration budget,
        long masterSeed,
        Duration load,
        boolean smoke,
        Path outDir,
        boolean jfr,
        long rate,
        int messageBytes,
        int depth,
        boolean standingQueue) {

    static final Duration DEFAULT_BUDGET = Duration.ofMinutes(30);
    static final Duration SMOKE_LOAD = Duration.ofSeconds(2);
    static final List<Long> SMOKE_SEEDS = List.of(1L, 2L, 3L, 4L, 5L, 6L);

    static Options parse(String[] args, long nowMillis) {
        OptionalLong seed = OptionalLong.empty();
        Duration budget = DEFAULT_BUDGET;
        long master = nowMillis;
        Duration load = null;
        boolean smoke = false;
        boolean jfr = false;
        long rate = 0;
        int messageBytes = 0;
        int depth = 0;
        boolean standingQueue = false;
        Path out = Path.of("build/random-bench");
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.equals("--smoke")) {
                smoke = true;
                continue;
            }
            if (a.equals("--standing-queue")) {
                standingQueue = true;
                continue;
            }
            if (a.equals("--jfr")) {
                jfr = true;
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
                case "--rate" -> rate = parseRate(v);
                case "--message-bytes" -> messageBytes = parsePositiveInt(a, v);
                case "--depth" -> depth = parsePositiveInt(a, v);
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
        if (smoke && rate > 0) {
            throw new IllegalArgumentException("--smoke와 --rate는 함께 쓸 수 없다");
        }
        if (rate == 0 && (messageBytes > 0 || depth > 0 || standingQueue)) {
            throw new IllegalArgumentException("--message-bytes, --depth, --standing-queue는 --rate와 함께 쓴다");
        }
        return new Options(
                seed,
                budget,
                master,
                load,
                smoke,
                out.toAbsolutePath(),
                jfr || rate > 0,
                rate,
                messageBytes,
                depth,
                standingQueue);
    }

    private static int parsePositiveInt(String option, String v) {
        long n = parseLong(option, v);
        if (n <= 0 || n > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(option + "는 양의 정수여야 한다: " + v);
        }
        return (int) n;
    }

    private static long parseRate(String v) {
        long n = parseLong("--rate", v);
        if (n <= 0) {
            throw new IllegalArgumentException("--rate는 양의 정수여야 한다: " + v);
        }
        return n;
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
