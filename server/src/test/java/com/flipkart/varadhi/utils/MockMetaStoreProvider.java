package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.flipkart.varadhi.spi.db.EventStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.db.OpStore;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mockito;

@Slf4j
public class MockMetaStoreProvider implements MetaStoreProvider {

    @Override
    public void init(MetaStoreOptions metaStoreOptions) {
        // No-op for mock implementation
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

    @Override
    public EventStore getEventStore() {
        return Mockito.mock(EventStore.class);
    }

    @Override
    public void close() {
        log.debug("Closing MockMetaStoreProvider");
    }
}
