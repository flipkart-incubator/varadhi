package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.utils.JsonMapper;
import lombok.Getter;
import lombok.Setter;

@Getter
public class VaradhiResource {

    private final String name;

    @Setter
    private int version;

    protected VaradhiResource(String name, int version) {
        this.name = name;
        this.version = version;
    }

}
