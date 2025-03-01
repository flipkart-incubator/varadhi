package com.flipkart.varadhi.controller.events;

import com.flipkart.varadhi.entities.CacheOperation;

public record MetaStoreEntityResult(Object state, CacheOperation cacheOperation) {

    public static MetaStoreEntityResult of(Object state) {
        return new MetaStoreEntityResult(state, CacheOperation.UPSERT);
    }

    public static MetaStoreEntityResult notFound() {
        return new MetaStoreEntityResult(null, CacheOperation.INVALIDATE);
    }
}
