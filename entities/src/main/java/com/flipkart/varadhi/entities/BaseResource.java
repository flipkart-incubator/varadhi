package com.flipkart.varadhi.entities;

import com.google.inject.Singleton;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;

import java.util.Set;
import java.util.stream.Collectors;

public interface BaseResource {

    @Singleton
    default void validate() {
        Set<ConstraintViolation<BaseResource>> violations =
                Validation.buildDefaultValidatorFactory().getValidator().validate(this);
        if (violations.isEmpty()) {
            return;
        }
        throw new IllegalArgumentException(violations.stream()
                .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                .collect(Collectors.joining(", ")));
    }
}
