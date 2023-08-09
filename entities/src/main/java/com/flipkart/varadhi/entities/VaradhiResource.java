package com.flipkart.varadhi.entities;

import jakarta.validation.constraints.Size;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode
public class VaradhiResource implements BaseResource {
    @Size(min = 5, max = 50, message = "Varadhi Resource Name Length must be between 5 and 50")
    private final String name;

    @Setter
    private int version;

    protected VaradhiResource(String name, int version) {
        this.name = name;
        this.version = version;
    }
}
