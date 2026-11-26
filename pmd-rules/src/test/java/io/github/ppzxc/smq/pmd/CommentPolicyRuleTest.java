package io.github.ppzxc.smq.pmd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import net.sourceforge.pmd.PMDConfiguration;
import net.sourceforge.pmd.PmdAnalysis;
import net.sourceforge.pmd.lang.document.FileId;
import net.sourceforge.pmd.lang.rule.RuleSet;
import net.sourceforge.pmd.reporting.RuleViolation;
import org.junit.jupiter.api.Test;

class CommentPolicyRuleTest {
    private List<RuleViolation> scan(String path, String source) { return scan(path, source, false); }

    private List<RuleViolation> scan(String path, String source, boolean toggles) {
        var rule = new CommentPolicyRule();
        rule.setLanguage(net.sourceforge.pmd.lang.java.JavaLanguageModule.getInstance());
        rule.setProperty(CommentPolicyRule.SPOTLESS_TOGGLES, toggles);
        rule.setName("STYLE-011");
        rule.setMessage("Implementation comment is forbidden");
        try (var pmd = PmdAnalysis.create(new PMDConfiguration())) {
            pmd.addRuleSet(RuleSet.forSingleRule(rule));
            pmd.files().addSourceFile(FileId.fromPathLikeString(path), source);
            var report = pmd.performAnalysisAndCollectReport();
            assertEquals(List.of(), report.getProcessingErrors());
            return report.getViolations();
        }
    }

    @Test void policyExceptionsRejectMissingAndExpiredApprovals() {
        var rule = new PolicyExceptionRule();
        rule.setLanguage(net.sourceforge.pmd.lang.java.JavaLanguageModule.getInstance());
        rule.setName("PolicyException");
        rule.setMessage("Invalid exception");
        try (var pmd = PmdAnalysis.create(new PMDConfiguration())) {
            pmd.addRuleSet(RuleSet.forSingleRule(rule));
            pmd.files().addSourceFile(FileId.fromPathLikeString("Example.java"), "class Example { @SuppressWarnings(\"PMD.SystemPrintln\") void run() {} @PolicyException(rule=\"PMD.SystemPrintln\", reason=\"CLI output\", approvedBy=\"owner\", reviewBy=\"2000-01-01\") void old() {} }");
            assertEquals(2, pmd.performAnalysisAndCollectReport().getViolations().size());
        }
    }

    @Test void policyExceptionRejectsBlankApprover() {
        var rule = new PolicyExceptionRule();
        rule.setLanguage(net.sourceforge.pmd.lang.java.JavaLanguageModule.getInstance());
        rule.setName("PolicyException");
        rule.setMessage("Invalid exception");
        try (var pmd = PmdAnalysis.create(new PMDConfiguration())) {
            pmd.addRuleSet(RuleSet.forSingleRule(rule));
            pmd.files().addSourceFile(FileId.fromPathLikeString("Example.java"),
                    "class Example { @PolicyException(rule=\"PMD.SystemPrintln\", reason=\"CLI output\", approvedBy=\"\", reviewBy=\"2099-01-01\") @SuppressWarnings(\"PMD.SystemPrintln\") void run() {} }");
            assertEquals(1, pmd.performAnalysisAndCollectReport().getViolations().size());
        }
    }

    @Test void policyExceptionRejectsBroadSuppressionEvenWithApproval() {
        var rule = new PolicyExceptionRule();
        rule.setLanguage(net.sourceforge.pmd.lang.java.JavaLanguageModule.getInstance());
        rule.setName("PolicyException");
        rule.setMessage("Invalid exception");
        try (var pmd = PmdAnalysis.create(new PMDConfiguration())) {
            pmd.addRuleSet(RuleSet.forSingleRule(rule));
            pmd.files().addSourceFile(FileId.fromPathLikeString("Example.java"),
                    "class Example { @PolicyException(rule=\"PMD\", reason=\"reason\", approvedBy=\"owner\", reviewBy=\"2099-01-01\") @SuppressWarnings(\"PMD\") void run() {} }");
            assertEquals(1, pmd.performAnalysisAndCollectReport().getViolations().size());
        }
    }

    @Test void policyExceptionAllowsMatchingUnexpiredApproval() {
        var rule = new PolicyExceptionRule();
        rule.setLanguage(net.sourceforge.pmd.lang.java.JavaLanguageModule.getInstance());
        rule.setName("PolicyException");
        rule.setMessage("Invalid exception");
        try (var pmd = PmdAnalysis.create(new PMDConfiguration())) {
            pmd.addRuleSet(RuleSet.forSingleRule(rule));
            pmd.files().addSourceFile(FileId.fromPathLikeString("Example.java"),
                    "class Example { @PolicyException(rule=\"PMD.SystemPrintln\", reason=\"CLI output\", approvedBy=\"owner\", reviewBy=\"2099-01-01\") @SuppressWarnings(\"PMD.SystemPrintln\") void run() {} }");
            var report = pmd.performAnalysisAndCollectReport();
            assertEquals(List.of(), report.getProcessingErrors());
            assertEquals(0, report.getViolations().size());
        }
    }

    @Test void onlyPublicApiAndLicenseAreAllowed() {
        String source = """
            /* Copyright Example */
            package sample;
            /** Public API */ public class Example {
                /** Public method */ public void good() {}
                /** Private method */ private void bad() {}
                /** Nested API */ protected class Nested { /** Visible */ public void good() {} }
                /** Private type */ private class Hidden { /** Hidden */ public void bad() {} }
            }
            """;
        assertEquals(List.of(5, 7, 7), scan("Example.java", source).stream().map(RuleViolation::getBeginLine).toList());
    }

    @Test void legalNoticeMayPrecedeFirstDeclaration() {
        assertEquals(0, scan("Example.java", "/* All rights reserved. */\npublic class Example {}").size());
        assertEquals(1, scan("Example.java", "/* All rights reserved. */\n/* Copyright Example */\npublic class Example {}").size());
        assertEquals(1, scan("Example.java", "/* TODO: fix license handling */\npublic class Example {}").size());
    }

    @Test void rejectsTodoInPublicDocsAndOrdinaryLicenseMention() {
        assertEquals(1, scan("Example.java", "/** TODO: remove workaround */ public class Example {} ").size());
        assertEquals(1, scan("Example.java", "/* Update license cache before startup. */ public class Example {} ").size());
        assertEquals(1, scan("Example.java", "/* Update copyright display. */ public class Example {} ").size());
        assertEquals(0, scan("Example.java", "/* Licensed under the Apache License, Version 2.0. */ public class Example {} ").size());
    }

    @Test void rejectsImplementationAndSuppressionComments() {
        String source = """
            public class Example {
              // TODO fix
              @SuppressWarnings("PMD")
              // NOPMD
              void run() { /** body */ int x = 1; /* explanation */ }
            }
            """;
        assertEquals(List.of(2, 4, 5, 5), scan("Example.java", source).stream().map(RuleViolation::getBeginLine).toList());
    }

    @Test void onlyRequiredErrorProneExplanationsAreAllowed() {
        assertEquals(1, scan("Example.java", "class Example { @SuppressWarnings(\"NonApiType\") // TODO fix\n void x() {} }").size());
        assertEquals(1, scan("Example.java", "class Example { @SuppressWarnings(\"NonApiType\") // TreeMap keeps messages in FIFO order for pulls.\n void x() {} }").size());
        assertEquals(0, scan("src/main/java/io/github/ppzxc/smq/storage/memory/MemoryStorage.java", "class Example { @SuppressWarnings(\"NonApiType\") // TreeMap keeps messages in FIFO order for pulls.\n void x() {} }").size());
    }

    @Test void parsesRealModuleDescriptor() {
        assertEquals(1, scan("module-info.java", "// TODO fix\nmodule example { requires static            org.jspecify; }").size());
    }

    @Test void packageDocsAndGeneratedFiles() {
        assertEquals(0, scan("package-info.java", "/** Package docs */ package sample;").size());
        assertEquals(1, scan("build/generated/Example.java", "// generated\nclass Example {} ").size());
        assertEquals(1, scan("Example.java", "/** Package docs */ package sample; class Example {}").size());
        assertEquals(1, scan("package-info.java", "package sample;\nclass Example { /** forbidden */ void method() {} }").size());
        assertEquals(1, scan("module-info.java", "// TODO forbidden\nmodule sample {} ").size());
    }

    @Test void rejectsUnpairedOrUnconfiguredSpotlessToggles() {
        assertEquals(2, scan("Example.java", "// spotless:off\nclass Example {}\n// spotless:on").size());
        assertEquals(1, scan("Example.java", "// spotless:off\nclass Example {}").size());
        assertEquals(0, scan("Example.java", "// spotless:off\nclass Example {}\n// spotless:on", true).size());
        assertEquals(1, scan("Example.java", "// spotless:off\nclass Example {}", true).size());
    }
}
