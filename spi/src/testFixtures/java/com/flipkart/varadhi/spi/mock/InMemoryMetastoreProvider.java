package com.flipkart.varadhi.spi.mock;

import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.db.OpStore;

public class InMemoryMetastoreProvider implements MetaStoreProvider {

    @Override
    public InMemoryMetaStore getMetaStore() {
        return new InMemoryMetaStore();
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void init(MetaStoreOptions metaStoreOptions) {
    }

    @Override
    public OpStore getOpStore() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOpStore'");
    }

    @Override
    public AssignmentStore getAssignmentStore() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAssignmentStore'");
    }
}
