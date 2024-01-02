package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class VaradhiResource extends VersionedEntity {
    public static final String NAME_SEPARATOR = ".";

    protected VaradhiResource(String name, int version) {
        super(name, version);
    }
}
