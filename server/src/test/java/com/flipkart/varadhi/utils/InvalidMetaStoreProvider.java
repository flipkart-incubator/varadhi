package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;

public class InvalidMetaStoreProvider implements MetaStoreProvider {
    @Override
    public void init(MetaStoreOptions MetaStoreOptions) {

    }

    @Override
    public MetaStore getMetaStore() {
        return null;
    }
}
