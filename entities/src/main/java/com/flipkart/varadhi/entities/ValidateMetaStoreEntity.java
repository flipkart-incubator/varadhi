package com.flipkart.varadhi.entities;


import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Constraint(validatedBy = MetaStoreEntityValidator.class)
@Target(TYPE)
@Retention(RUNTIME)
public @interface ValidateMetaStoreEntity {

    String NAME_VALIDATION_PATTERN = "[a-z]{1}[a-z0-9_-]+[a-z0-9]{1}";

    String regexp() default NAME_VALIDATION_PATTERN; //set to "" for disabling pattern check.

    int min() default 3; //set to -1 for disabling minimum length check. Change default expression as appropriate.

    int max() default 32; //set to -1 for disabling maximum length check.

    boolean allowNullOrBlank() default false;

    String message() default "Invalid name attribute.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
