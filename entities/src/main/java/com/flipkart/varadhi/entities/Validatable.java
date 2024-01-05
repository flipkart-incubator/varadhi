package com.flipkart.varadhi.entities;

import java.util.List;

public interface Validatable {
    default void validate() {
        List<String> failures = Validator.validate(this);
        if (failures.isEmpty()) {
            return;
        }
        throw new IllegalArgumentException(String.join(",", failures));
    }
}
