package com.flipkart.varadhi.common.events;

public record MetaStoreEntityResult<T>(T state, ResourceOperation resourceOperation) {

    public static <T> MetaStoreEntityResult<T> of(T state) {
        return new MetaStoreEntityResult<>(state, ResourceOperation.UPSERT);
    }

    public static <T> MetaStoreEntityResult<T> notFound() {
        return new MetaStoreEntityResult<>(null, ResourceOperation.INVALIDATE);
    }
}
