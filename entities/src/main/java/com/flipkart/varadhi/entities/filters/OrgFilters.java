package com.flipkart.varadhi.entities.filters;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import lombok.Getter;

import java.util.Map;

@Getter
public class OrgFilters extends MetaStoreEntity {
    private Map<String, Condition> filters;

    protected OrgFilters(int version) {
        super("NamedFilters", version);
    }
}
