package com.flipkart.varadhi.db;

public interface MetaStoreProvider {
    void init(MetaStoreOptions MetaStoreOptions);

    MetaStore getMetaStore();
}
