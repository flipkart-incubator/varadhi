package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.db.OpStore;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InvalidMetaStoreProvider implements MetaStoreProvider {

    @Override
    public void init(MetaStoreOptions metaStoreOptions) {
        // No-op for mock implementation
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

    @Override
    public void close() {
        log.debug("Closing InvalidMetaStoreProvider");
    }
}
