package io.github.ppzxc.smq.harness;

import java.util.ArrayList;
import java.util.List;

final class BacklogCommand {

    private BacklogCommand() {}

    static int execute(final String[] args) {
        BacklogOptions options = null;
        final List<BacklogRunner.Sample> results = new ArrayList<>();
        try {
            options = BacklogOptions.parse(args);
            new BacklogRunner().run(options, results);
            BacklogReport.write(options, results, "SUCCESS", null);
            System.out.println("smq backlog completed " + results.size() + " measurements");
            return 0;
        } catch (final Exception exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            System.err.println("smq backlog failed: " + exception.getMessage());
            if (options != null) {
                try {
                    BacklogReport.write(options, results, "FAILURE", exception.toString());
                } catch (final Exception reportException) {
                    System.err.println("could not write backlog report: " + reportException.getMessage());
                }
            }
            return 1;
        }
    }
}
