package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.KeyProvider;

import java.util.List;

public interface MetaStore<T extends KeyProvider> {
    T get(String resourcePath, Class<T> clazz);

    void create(T entity);

    boolean exists(String resourceKey);

    void delete(String resourceKey);

    List<String> list(String resourceKey);

}
