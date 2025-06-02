package com.flipkart.varadhi.entities;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.regex.Pattern;

public class ResourceValidator implements ConstraintValidator<ValidateResource, Versioned> {
    String regExp;
    int minLength;
    int maxLength;
    boolean allowNullOrBlank;

    @Override
    public void initialize(ValidateResource constraintAnnotation) {
        this.regExp = constraintAnnotation.regexp();
        this.maxLength = constraintAnnotation.max();
        this.minLength = constraintAnnotation.min();
        this.allowNullOrBlank = constraintAnnotation.allowNullOrBlank();
    }

    @Override
    public boolean isValid(Versioned value, ConstraintValidatorContext context) {
        if (null == value.getName() || value.getName().isBlank()) {
            return allowNullOrBlank;
        }
        int nameLength = value.getName().length();
        if (-1 != minLength && nameLength < minLength) {
            return false;
        }
        if (-1 != maxLength && nameLength > maxLength) {
            return false;
        }
        if (!regExp.isBlank()) {
            return Pattern.matches(regExp, value.getName());
        }
        return true;
    }

}
