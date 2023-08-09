package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class Org extends VaradhiResource {
    public Org(
            String name,
            int version
    ) {
        super(name, version);
    }
}
