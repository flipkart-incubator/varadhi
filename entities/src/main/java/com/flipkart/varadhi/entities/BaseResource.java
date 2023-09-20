package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.exceptions.ArgumentException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;

import java.util.Set;
import java.util.stream.Collectors;

public interface BaseResource {

    default void validate() {
        Set<ConstraintViolation<BaseResource>> violations =
                Validation.buildDefaultValidatorFactory().getValidator().validate(this);
        if (violations.isEmpty()) {
            return;
        }
        throw new ArgumentException(violations.stream()
                .map(violation ->
                        violation.getPropertyPath() == null || violation.getPropertyPath().toString().isBlank() ?
                                violation.getMessage() :
                                violation.getPropertyPath().toString() + ": " + violation.getMessage())
                .collect(Collectors.joining(", ")));
    }

}
