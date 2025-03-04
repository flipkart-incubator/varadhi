package com.flipkart.varadhi.common.utils;

import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.spi.db.*;
import org.mockito.Mockito;

public class MockMetaStoreProvider implements MetaStoreProvider {
    @Override
    public void init(MetaStoreOptions MetaStoreOptions) {

    }

    @Override
    public MetaStore getMetaStore() {
        return Mockito.mock(VaradhiMetaStore.class);
    }

    @Override
    public OpStore getOpStore() {
        return Mockito.mock(OpStore.class);
    }

    @Override
    public AssignmentStore getAssignmentStore() {
        return Mockito.mock(AssignmentStore.class);
    }
}
