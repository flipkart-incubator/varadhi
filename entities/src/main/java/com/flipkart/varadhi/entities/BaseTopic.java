package com.flipkart.varadhi.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@AllArgsConstructor
public abstract class BaseTopic {
    private final String name;
    @Setter
    private int version;
}
