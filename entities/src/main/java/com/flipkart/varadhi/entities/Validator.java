package com.flipkart.varadhi.entities;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;

import java.util.List;
import java.util.Set;

public class Validator {

    /**
     * TODO: need for customization based on external config.
     */
    static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    static final jakarta.validation.Validator validator = factory.getValidator();

    public static <T> List<String> validate(T object) {
        Set<ConstraintViolation<T>> violations = validator.validate(object);
        return violations.stream()
                .map(violation ->
                        violation.getPropertyPath() == null || violation.getPropertyPath().toString().isBlank() ?
                                violation.getMessage() :
                                violation.getPropertyPath().toString() + ": " + violation.getMessage()).toList();
    }
}
