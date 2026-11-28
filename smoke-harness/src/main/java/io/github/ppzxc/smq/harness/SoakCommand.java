package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Smq;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class SoakCommand {

    private SoakCommand() {}

    static int execute(final String[] args) {
        final SoakOptions options;
        try {
            options = SoakOptions.parse(args);
        } catch (final IllegalArgumentException exception) {
            System.err.println("smq soak failed: " + exception.getMessage());
            return 2;
        }

        final List<SoakResult> results = new ArrayList<>();
        final List<String> diagnostics = new ArrayList<>();
        final String workloadDigest = SoakWorkload.digest(options);
        try {
            Files.createDirectories(options.workDir());
            for (final var durability : options.durabilities()) {
                final Path sessionDir = Files.createTempDirectory(
                        options.workDir(),
                        "seed-" + options.seed() + "-" + durability.name().toLowerCase(Locale.ROOT) + "-");
                final Path database = sessionDir.resolve("smq.db");
                try (Smq smq = Smq.sqlite(database).durability(durability).open()) {
                    results.addAll(new SoakRunner(options, durability, smq).run());
                }
                if (!results.getLast().passed()) {
                    break;
                }
            }
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            diagnostics.add("interrupted: " + exception);
        } catch (final Exception exception) {
            diagnostics.add(exception.toString());
        }

        try {
            SoakReport.write(options, workloadDigest, results, diagnostics);
        } catch (final Exception exception) {
            System.err.println("could not write soak report: " + exception.getMessage());
            return 1;
        }
        final boolean failed = !diagnostics.isEmpty() || results.stream().anyMatch(result -> !result.passed());
        if (failed) {
            System.err.println("smq soak failed; report: " + options.output());
            results.stream()
                    .flatMap(result -> result.violations().stream())
                    .forEach(violation -> System.err.println("  " + violation.invariant() + " seed=" + violation.seed()
                            + " round=" + violation.round()));
            diagnostics.forEach(diagnostic -> System.err.println("  " + diagnostic));
            return 1;
        }
        System.out.println("smq soak passed " + results.size() + " seeded rounds; report: " + options.output());
        return 0;
    }
}
