package com.flipkart.varadhi.spi.db;

public interface MetaStoreProvider {
    void init(MetaStoreOptions MetaStoreOptions);

    MetaStore getMetaStore();
}
