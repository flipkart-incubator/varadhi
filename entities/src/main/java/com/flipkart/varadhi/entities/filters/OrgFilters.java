package com.flipkart.varadhi.entities.filters;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.Map;

@Getter
// TODO: rename it to NamedFilters
public class OrgFilters extends MetaStoreEntity {

    /**
     * Map of filter name to filter conditions.
     */
    private final Map<String, Condition> filters;

    @JsonCreator
    public OrgFilters(@JsonProperty ("version") int version, @JsonProperty ("filters") Map<String, Condition> filters) {
        super("NamedFilters", version, MetaStoreEntityType.ORG_FILTER);
        this.filters = ImmutableMap.copyOf(filters);
    }
}
