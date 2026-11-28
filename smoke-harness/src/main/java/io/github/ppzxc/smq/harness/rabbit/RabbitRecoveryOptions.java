package io.github.ppzxc.smq.harness.rabbit;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record RabbitRecoveryOptions(String host, int port, String container, Path output, Path workDir) {

    public static RabbitRecoveryOptions parse(final String[] args, final int start) {
        final Map<String, String> values = new HashMap<>();
        for (int index = start; index < args.length; index += 2) {
            if (index + 1 == args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected --option value pairs");
            }
            final String key = args[index].substring(2);
            if (!List.of("host", "port", "container", "output", "work-dir").contains(key)) {
                throw new IllegalArgumentException("unknown option --" + key);
            }
            if (values.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate --" + key);
            }
        }
        final int port = Integer.parseInt(values.getOrDefault("port", "5672"));
        if (port < 1) {
            throw new IllegalArgumentException("--port must be positive");
        }
        if (port > 65535) {
            throw new IllegalArgumentException("--port must not exceed 65535");
        }
        final String container = values.getOrDefault("container", "smq-rabbitmq-441");
        if (!container.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("--container must be a Docker container name");
        }
        return new RabbitRecoveryOptions(
                values.getOrDefault("host", "localhost"),
                port,
                container,
                Path.of(values.getOrDefault("output", "build/smoke-harness/rabbit-recovery-report.json"))
                        .toAbsolutePath(),
                Path.of(values.getOrDefault("work-dir", "build/smoke-harness/rabbit-recovery-work"))
                        .toAbsolutePath());
    }
}
