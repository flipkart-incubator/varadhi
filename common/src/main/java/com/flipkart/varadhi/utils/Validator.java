package com.flipkart.varadhi.utils;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;

import java.util.List;
import java.util.Set;

public class Validator {

    public static <T> List<String> validate(T object) {
        Set<ConstraintViolation<T>> violations =
                Validation.buildDefaultValidatorFactory().getValidator().validate(object);
        return violations.stream()
                .map(violation ->
                        violation.getPropertyPath() == null || violation.getPropertyPath().toString().isBlank() ?
                                violation.getMessage() :
                                violation.getPropertyPath().toString() + ": " + violation.getMessage()).toList();
    }
}
