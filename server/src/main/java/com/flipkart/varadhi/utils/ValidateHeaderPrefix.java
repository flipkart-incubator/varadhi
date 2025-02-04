package com.flipkart.varadhi.utils;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Constraint(validatedBy = PrefixBasedHeaderValidator.class)
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidateHeaderPrefix {
    String message() default "Header prefix validation failed";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}

