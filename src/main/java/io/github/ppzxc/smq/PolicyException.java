package io.github.ppzxc.smq;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
@Repeatable(PolicyExceptions.class)
public @interface PolicyException {
    String rule();

    String reason();

    String approvedBy();

    String reviewBy();
}
