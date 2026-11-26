package io.github.ppzxc.smq.pmd;

import java.util.Locale;
import net.sourceforge.pmd.lang.ast.Node;
import net.sourceforge.pmd.lang.java.ast.ASTCompilationUnit;
import net.sourceforge.pmd.lang.java.ast.ASTPackageDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTTypeDeclaration;
import net.sourceforge.pmd.lang.java.ast.JavaComment;
import net.sourceforge.pmd.lang.java.ast.JavadocComment;
import net.sourceforge.pmd.lang.java.ast.JavadocCommentOwner;
import net.sourceforge.pmd.lang.java.ast.ModifierOwner;
import net.sourceforge.pmd.lang.java.ast.ModifierOwner.Visibility;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;

public class CommentPolicyRule extends AbstractJavaRule {
    public static final net.sourceforge.pmd.properties.PropertyDescriptor<Boolean> SPOTLESS_TOGGLES =
        net.sourceforge.pmd.properties.PropertyFactory.booleanProperty("spotlessToggles").desc("Spotless toggleOffOn is configured").defaultValue(false).build();
    public CommentPolicyRule() { definePropertyDescriptor(SPOTLESS_TOGGLES); }

    @Override public Object visit(ASTCompilationUnit unit, Object data) {
        String path = unit.getTextDocument().getFileId().getAbsolutePath().replace('\\', '/');
        int firstDeclaration = unit.getFirstChild() == null ? Integer.MAX_VALUE
                : unit.getFirstChild().getTextRegion().getStartOffset();
        boolean licenseSeen = false;
        java.util.Set<JavaComment> paired = new java.util.HashSet<>();
        JavaComment off = null;
        if (getProperty(SPOTLESS_TOGGLES)) {
            for (JavaComment comment : unit.getComments()) {
                String text = comment.getText().toString().trim();
                if (text.equals("// spotless:off")) off = off == null ? comment : null;
                else if (text.equals("// spotless:on") && off != null) {
                    paired.add(off); paired.add(comment); off = null;
                }
            }
        }
        String[] lines = unit.getTextDocument().getText().toString().split("\\R", -1);
        for (JavaComment comment : unit.getComments()) {
            if (paired.contains(comment)) continue;
            String text = comment.getText().toString();
            String lower = text.toLowerCase(Locale.ROOT);
            boolean license = !comment.isSingleLine() && !licenseSeen
                    && comment.getToken().getRegion().getStartOffset() < firstDeclaration
                    && !lower.contains("todo") && !lower.contains("fixme")
                    && lower.matches("(?s)/\\*+\\s*(?:\\*\\s*)?(?:copyright\\b|spdx-license-identifier:|all rights reserved\\b|licensed under\\b).*");
            if (license) { licenseSeen = true; continue; }
            if (!lower.contains("todo") && !lower.contains("fixme") && comment instanceof JavadocComment doc
                    && ((doc.getOwner() instanceof ASTPackageDeclaration && (path.endsWith("/package-info.java") || path.equals("package-info.java")))
                            || publicApi(doc))) continue;
            int line = comment.getReportLocation().getStartLine();
            String prefix = lines[line - 1].substring(0, Math.min(lines[line - 1].length(), comment.getReportLocation().getStartColumn() - 1));
            if ((path.endsWith("/io/github/ppzxc/smq/storage/memory/MemoryStorage.java") && prefix.contains("@SuppressWarnings(\"NonApiType\")") && text.trim().equals("// TreeMap keeps messages in FIFO order for pulls."))
                    || (path.endsWith("/io/github/ppzxc/smq/storage/sqlite/TransactionExecutor.java") && prefix.contains("@SuppressWarnings(\"ReferenceEquality\")") && text.trim().equals("// Suppress self-suppression by comparing Throwable identity."))
                    || ((path.endsWith("/io/github/ppzxc/smq/Message.java") || path.endsWith("/io/github/ppzxc/smq/OutgoingMessage.java") || path.endsWith("/io/github/ppzxc/smq/storage/NewMessage.java") || path.endsWith("/io/github/ppzxc/smq/storage/sqlite/SqliteStorage.java")) && prefix.contains("@SuppressWarnings(\"ArrayRecordComponent\")") && java.util.Set.of(
                        "// Payload is copied at construction and access.",
                        "// Internal storage input retains its caller-owned payload.",
                        "// This private record carries a database-owned payload.").contains(text.trim()))) continue;
            asCtx(data).addViolationNoSuppress(comment, unit.getAstInfo(), "STYLE-011: implementation comment is forbidden");
        }
        return data;
    }

    private static boolean publicApi(JavadocComment doc) {
        JavadocCommentOwner owner = doc.getOwner();
        if (!(owner instanceof ModifierOwner modifiers) || !visible(modifiers)) return false;
        for (Node node = owner.getParent(); node != null; node = node.getParent()) {
            if (node instanceof ASTTypeDeclaration type && !visible(type)) return false;
        }
        return true;
    }

    private static boolean visible(ModifierOwner owner) {
        return owner.getEffectiveVisibility() == Visibility.V_PUBLIC || owner.getEffectiveVisibility() == Visibility.V_PROTECTED;
    }
}
