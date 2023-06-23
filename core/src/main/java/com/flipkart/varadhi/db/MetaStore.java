package com.flipkart.varadhi.db;


public interface MetaStore<T> {

    // TODO:: Discuss/Evaluate API semantics for get/exists, specific resource should implement only one
    // based on its uniqueness i.e. relative or global.
    // or any alternate options here.
    // get(String parentKey, String resourceKey) and get(String resourceKey)

    T get(String parent, String resourcePath);

    T create(T entity);

    boolean exists(String parent, String resourceName);

}
