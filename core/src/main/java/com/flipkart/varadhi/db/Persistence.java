package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.KeyProvider;

public interface Persistence<T extends KeyProvider> {
    T get(String resourcePath, Class<T> clazz);

    void create(T entity);

    boolean exists(String resourceKey);

}
