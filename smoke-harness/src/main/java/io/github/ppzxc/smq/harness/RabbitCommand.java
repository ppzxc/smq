package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.harness.rabbit.RabbitOptions;
import io.github.ppzxc.smq.harness.rabbit.RabbitPlan;
import io.github.ppzxc.smq.harness.rabbit.RabbitRecoveryOptions;
import io.github.ppzxc.smq.harness.rabbit.RabbitRecoveryReport;
import io.github.ppzxc.smq.harness.rabbit.RabbitRecoveryRunner;
import io.github.ppzxc.smq.harness.rabbit.RabbitReport;
import io.github.ppzxc.smq.harness.rabbit.RabbitRunner;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

final class RabbitCommand {

    private RabbitCommand() {}

    static int execute(final String[] args) {
        try {
            if (args.length < 2) {
                throw new IllegalArgumentException("usage: rabbit plan|run|recovery");
            }
            if ("plan".equals(args[1])) {
                final Map<String, String> options = parse(args, 2);
                final String output = options.getOrDefault("output", "build/smoke-harness/rabbit-plan.json");
                RabbitPlan.write(Path.of(output).toAbsolutePath());
                System.out.println("RabbitMQ plan written to " + Path.of(output).toAbsolutePath());
                return 0;
            }
            if ("run".equals(args[1])) {
                final RabbitOptions options = RabbitOptions.parse(args, 2);
                final RabbitRunner runner = new RabbitRunner();
                final var results = new ArrayList<RabbitRunner.Sample>();
                try {
                    runner.run(options, results);
                    RabbitReport.write(options, results, runner.actualServerVersion(), "SUCCESS", null);
                    System.out.println("RabbitMQ benchmark completed "
                            + results.stream()
                                    .mapToLong(RabbitRunner.Sample::completedMessages)
                                    .sum()
                            + " messages");
                    return 0;
                } catch (final Exception exception) {
                    if (exception instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    RabbitReport.write(options, results, runner.actualServerVersion(), "FAILURE", exception.toString());
                    System.err.println("RabbitMQ benchmark failed: " + exception.getMessage());
                    return 1;
                }
            }
            if ("recovery".equals(args[1])) {
                final RabbitRecoveryOptions options = RabbitRecoveryOptions.parse(args, 2);
                final RabbitRecoveryRunner runner = new RabbitRecoveryRunner(options);
                final var observations = new ArrayList<RabbitRecoveryRunner.Observation>();
                try {
                    runner.run(observations);
                    RabbitRecoveryReport.write(options, runner, observations, "SUCCESS", null);
                    System.out.println(
                            "RabbitMQ recovery scenarios completed " + observations.size() + " observations");
                    return 0;
                } catch (final Exception exception) {
                    if (exception instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    exception.printStackTrace(System.err);
                    RabbitRecoveryReport.write(options, runner, observations, "FAILURE", exception.toString());
                    System.err.println("RabbitMQ recovery failed: " + exception.getMessage());
                    return 1;
                }
            }
            throw new IllegalArgumentException("unsupported rabbit command: " + args[1]);
        } catch (final Exception exception) {
            System.err.println("rabbit harness failed: " + exception.getMessage());
            return 1;
        }
    }

    private static Map<String, String> parse(final String[] args, final int start) {
        final Map<String, String> options = new HashMap<>();
        for (int index = start; index < args.length; index += 2) {
            if (index + 1 == args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected --option value pairs");
            }
            final String key = args[index].substring(2);
            if (!"output".equals(key)) {
                throw new IllegalArgumentException("unknown option --" + key);
            }
            if (options.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate --" + key);
            }
        }
        return Map.copyOf(options);
    }
}
