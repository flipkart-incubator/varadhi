package com.flipkart.varadhi.entities.filters;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;

import java.util.Map;

@Getter
public class OrgFilters extends MetaStoreEntity {

    private final Map<String, Condition> filters;

    public OrgFilters(int version, Map<String, Condition> filters) {
        super("NamedFilters", version);
        this.filters = ImmutableMap.copyOf(filters);
    }
}
