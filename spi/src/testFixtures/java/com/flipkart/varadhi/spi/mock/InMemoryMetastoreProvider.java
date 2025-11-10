package com.flipkart.varadhi.spi.mock;

import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.db.OpStore;

public class InMemoryMetastoreProvider implements MetaStoreProvider {

    private final InMemoryMetaStore metastore = new InMemoryMetaStore();

    @Override
    public void init(MetaStoreOptions metaStoreOptions) {
    }

    @Override
    public InMemoryMetaStore getMetaStore() {
        return metastore;
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

    @Override
    public void close() throws Exception {
    }
}
