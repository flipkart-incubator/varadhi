package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import org.mockito.Mockito;

public class MockMetaStoreProvider implements MetaStoreProvider {
    @Override
    public void init(MetaStoreOptions MetaStoreOptions) {

    }

    @Override
    public MetaStore getMetaStore() {
        return Mockito.mock(VaradhiMetaStore.class);
    }
}
