package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.spi.db.*;

public class InvalidMetaStoreProvider implements MetaStoreProvider {
    @Override
    public void init(MetaStoreOptions MetaStoreOptions) {

    }

    @Override
    public MetaStore getMetaStore() {
        return null;
    }

    @Override
    public OpStore getOpStore() {
        return null;
    }

    @Override
    public AssignmentStore getAssignmentStore() {
        return null;
    }
}
