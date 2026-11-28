package io.github.ppzxc.smq;

import java.io.OutputStream;
import java.nio.file.Path;

public final class LockHolderMain {

    private LockHolderMain() {}

    @PolicyException(
            rule = "PMD.SystemPrintln",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(final String[] args) throws Exception {
        try (Smq ignored = Smq.sqlite(Path.of(args[0])).open()) {
            System.out.println("READY");
            System.out.flush();
            System.in.transferTo(OutputStream.nullOutputStream());
        }
    }
}
