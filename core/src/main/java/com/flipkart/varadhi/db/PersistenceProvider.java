package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.KeyProvider;


public interface PersistenceProvider {
    void init(DBOptions DBOptions);

    <T extends KeyProvider> Persistence<T> getPersistence();
}
