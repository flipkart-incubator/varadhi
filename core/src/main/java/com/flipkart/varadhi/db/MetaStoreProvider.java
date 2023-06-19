package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.KeyProvider;


public interface MetaStoreProvider {
    void init(MetaStoreOptions MetaStoreOptions);

    <T extends KeyProvider> MetaStore<T> getMetaStore();
}
