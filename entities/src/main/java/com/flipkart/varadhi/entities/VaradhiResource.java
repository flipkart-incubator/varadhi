package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class VaradhiResource extends VersionedEntity {
    public static final String NAME_SEPARATOR = ".";

    protected VaradhiResource(String name, int version) {
        super(name, version);
    }
}
