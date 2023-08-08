package com.flipkart.varadhi.entities;

import com.google.inject.Singleton;
import lombok.Getter;
import lombok.Setter;

import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.constraints.Size;

@Getter
@Valid
public class VaradhiResource {

    @Size(min = 5, max = 50, message = "Varadhi Resource Name Length must be between 5 and 50")
    private final String name;

    @Setter
    private int version;

    protected VaradhiResource(String name, int version) {
        this.name = name;
        this.version = version;
    }

    @Singleton
    public Validator getValidator() {
        return Validation.buildDefaultValidatorFactory().getValidator();
    }
}
