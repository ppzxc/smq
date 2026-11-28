package io.github.ppzxc.smq.harness;

import java.util.ArrayList;
import java.util.List;

public final class Main {

    private Main() {}

    public static void main(final String[] args) {
        final int exitCode = execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static int execute(final String[] args) {
        if (args.length > 0 && "--worker".equals(args[0])) {
            return RecoveryWorker.execute(args);
        }
        if (args.length > 0 && "recovery".equals(args[0])) {
            return RecoveryCommand.execute(args);
        }
        if (args.length > 0 && "backlog".equals(args[0])) {
            return BacklogCommand.execute(args);
        }

        Options options = null;
        final List<SmokeRunner.Sample> results = new ArrayList<>();
        try {
            options = Options.parse(args);
            new SmokeRunner().run(options, results);
            JsonReport.write(options, results, "SUCCESS", null);
            System.out.println("smq smoke completed "
                    + results.stream().mapToLong(SmokeRunner.Sample::completed).sum() + " messages");
            return 0;
        } catch (final Exception exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            System.err.println("smq smoke failed: " + exception.getMessage());
            if (options != null) {
                try {
                    JsonReport.write(options, results, "FAILURE", exception.toString());
                } catch (final Exception reportException) {
                    System.err.println("could not write failure report: " + reportException.getMessage());
                }
            }
            return 1;
        }
    }
}
