package com.flipkart.varadhi.entities.filters;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import lombok.Getter;

import java.util.Map;

@Getter
public class OrgFilters extends MetaStoreEntity {
    Map<String, Condition> filters;

    protected OrgFilters(String name, int version) {
        super(name, version);
    }
}
