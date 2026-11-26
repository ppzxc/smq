package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Test;

class PolicyExceptionTest {
    @Test
    void policyExceptionHasClassRetentionAndCanRepeatOnDeclarations() {
        assertEquals(
                RetentionPolicy.CLASS,
                PolicyException.class.getAnnotation(Retention.class).value());
        assertEquals(
                java.util.Set.of(ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD),
                java.util.Set.of(
                        PolicyException.class.getAnnotation(Target.class).value()));
        assertNotNull(PolicyException.class.getAnnotation(java.lang.annotation.Repeatable.class));
    }
}
