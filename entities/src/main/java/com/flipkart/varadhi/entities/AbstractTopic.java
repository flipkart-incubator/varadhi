package com.flipkart.varadhi.entities;

public class AbstractTopic extends VaradhiResource {

    public static String NAME_SEPARATOR = ".";

    public AbstractTopic(String name, int version) {
        super(name, version);
    }
}
