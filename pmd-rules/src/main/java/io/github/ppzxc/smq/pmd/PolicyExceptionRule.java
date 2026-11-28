package io.github.ppzxc.smq.pmd;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import net.sourceforge.pmd.lang.java.ast.ASTAnnotation;
import net.sourceforge.pmd.lang.java.ast.ASTModifierList;
import net.sourceforge.pmd.lang.java.ast.ASTStringLiteral;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;

public class PolicyExceptionRule extends AbstractJavaRule {
    @Override
    public Object visit(ASTModifierList modifiers, Object data) {
        var annotations = modifiers.children(ASTAnnotation.class).toList();
        Set<String> suppressed = new HashSet<>();
        Set<String> approved = new HashSet<>();
        var policyAnnotations = new ArrayList<ASTAnnotation>();
        for (var annotation : annotations) {
            if (annotation.getSimpleName().equals("SuppressWarnings")) {
                annotation.descendants(ASTStringLiteral.class).forEach(value -> {
                    if ((value.getConstValue().equals("PMD") || value.getConstValue().startsWith("PMD."))) suppressed.add(value.getConstValue());
                });
            }
            if (annotation.getSimpleName().equals("PolicyException")) {
                policyAnnotations.add(annotation);
            } else if (annotation.getSimpleName().equals("PolicyExceptions")) {
                annotation.descendants(ASTAnnotation.class)
                        .filter(nested -> nested.getSimpleName().equals("PolicyException"))
                        .forEach(policyAnnotations::add);
            }
        }
        for (var annotation : policyAnnotations) {
            var rule = annotation.getAttribute("rule");
            var reviewBy = annotation.getAttribute("reviewBy");
            if (!(annotation.getAttribute("approvedBy") instanceof ASTStringLiteral approver)
                    || approver.getConstValue().isBlank()
                    || !(annotation.getAttribute("reason") instanceof ASTStringLiteral reason)
                    || reason.getConstValue().isBlank()) {
                asCtx(data).addViolationNoSuppress(annotation, annotation.getAstInfo(), "Policy exception needs an approver and reason");
            }
            if (rule instanceof ASTStringLiteral name) approved.add(name.getConstValue());
            try {
                if (!(reviewBy instanceof ASTStringLiteral date)
                        || LocalDate.parse(date.getConstValue()).isBefore(LocalDate.now())) {
                    asCtx(data).addViolationNoSuppress(annotation, annotation.getAstInfo(), "Policy exception is expired or has an invalid review date");
                }
            } catch (DateTimeParseException ex) {
                asCtx(data).addViolationNoSuppress(annotation, annotation.getAstInfo(), "Policy exception has an invalid review date");
            }
        }
        if (suppressed.contains("PMD") || !approved.containsAll(suppressed)) {
            asCtx(data).addViolationNoSuppress(modifiers, modifiers.getAstInfo(), "PMD suppression requires a matching PolicyException");
        }
        return super.visit(modifiers, data);
    }
}
