package io.github.ppzxc.smq.harness;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class StorageLossCommand {

    private StorageLossCommand() {}

    static int execute(final String[] args) {
        StorageLossOptions options;
        try {
            options = StorageLossOptions.parse(args);
        } catch (final Exception exception) {
            System.err.println("smq storage-loss failed: " + exception.getMessage());
            return 1;
        }

        final List<StorageLossResult> results = new ArrayList<>();
        String status = "SUCCESS";
        String runDir = "";
        String ledgerPath = "";
        String notRunReason = null;
        String diagnostic = null;
        try {
            Files.createDirectories(options.workDir());
            final Path run = Files.createTempDirectory(options.workDir(), "storage-loss-");
            final Path stores = Files.createDirectory(run.resolve("stores"));
            final Path ledger = run.resolve("completion-ledger.txt");
            runDir = run.toString();
            ledgerPath = ledger.toString();
            final StorageLossController controller = new StorageLossController(stores, ledger, options);
            controller.observeProcessCrashOutcomes(results);
            if (options.simulateStorageLoss()) {
                controller.verifySimulatedStorageLoss(results);
            } else {
                status = options.requireStorageLoss() ? "FAILURE" : "SKIPPED";
                notRunReason = "simulated storage-loss run was disabled by --simulate-storage-loss false";
                results.add(controller.notRunSimulation(notRunReason));
            }
        } catch (final Exception exception) {
            status = "FAILURE";
            diagnostic = exception.toString();
            if (options.simulateStorageLoss() && !hasSimulationRun(results)) {
                notRunReason = "simulated storage-loss verification did not run: " + exception.getMessage();
            }
            System.err.println("smq storage-loss failed: " + exception.getMessage());
        }

        try {
            StorageLossReport.write(options, results, status, runDir, ledgerPath, notRunReason, diagnostic);
        } catch (final Exception exception) {
            System.err.println("could not write storage-loss report: " + exception.getMessage());
            return 1;
        }
        if ("FAILURE".equals(status)) {
            return 1;
        }
        System.out.println("smq storage-loss completed with status " + status);
        return 0;
    }

    private static boolean hasSimulationRun(final List<StorageLossResult> results) {
        return results.stream()
                .anyMatch(result -> StorageLossResult.SIMULATED_STORAGE_LOSS.equals(result.faultType)
                        && !"NOT_RUN".equals(result.status));
    }
}
