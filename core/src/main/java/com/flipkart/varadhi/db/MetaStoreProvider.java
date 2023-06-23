package com.flipkart.varadhi.db;

public interface MetaStoreProvider {
    void init(MetaStoreOptions MetaStoreOptions);

    <T extends MetaStore<?>> T getMetaStore(Class<?> clazz);
}
