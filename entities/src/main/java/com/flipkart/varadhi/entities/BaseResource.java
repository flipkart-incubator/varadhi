package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.exceptions.ArgumentException;
import com.flipkart.varadhi.utils.Validator;

import java.util.List;

public interface BaseResource {

    //    @Singleton
    default void validate() {
        List<String> failures = Validator.validate(this);
        if (failures.isEmpty()) {
            return;
        }
        throw new ArgumentException(String.join(",", failures));
    }
}
