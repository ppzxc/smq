package io.github.ppzxc.smq.harness;

import java.util.ArrayList;
import java.util.List;

final class RecoveryCommand {

    private RecoveryCommand() {}

    static int execute(final String[] args) {
        RecoveryOptions options;
        try {
            options = RecoveryOptions.parse(args);
        } catch (final Exception exception) {
            System.err.println("smq recovery failed: " + exception.getMessage());
            return 1;
        }

        final List<RecoveryResult> results = new ArrayList<>();
        try {
            new RecoveryController().run(options, results);
            RecoveryReport.write(options, results, "SUCCESS", null);
            System.out.println("smq recovery verified " + results.size() + " crash scenarios");
            return 0;
        } catch (final Exception exception) {
            System.err.println("smq recovery failed: " + exception.getMessage());
            try {
                RecoveryReport.write(options, results, "FAILURE", exception.toString());
            } catch (final Exception reportException) {
                System.err.println("could not write recovery report: " + reportException.getMessage());
            }
            return 1;
        }
    }
}
