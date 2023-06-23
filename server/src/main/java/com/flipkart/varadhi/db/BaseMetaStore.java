package com.flipkart.varadhi.db;

public class BaseMetaStore {
    private static final String BASE_PATH = "/varadhi/entities";
    protected static final int INITIAL_VERSION = 0;
    protected ZKMetaStore zkMetaStore;

    protected BaseMetaStore(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
    }

    protected String constructPath(String... components) {
        return String.join("/", BASE_PATH, String.join("/", components));
    }
}
